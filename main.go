package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/luis-ale-117/cella"
	"github.com/mackerelio/go-osstat/memory"
)

const (
	WAIT_TIME           = 3  // seconds
	MAX_PROCESSING_TIME = 30 // seconds
	MAX_MEMORY_USAGE    = 90 // percentage
)

func main() {
	var db *sql.DB
	var err error

	// Handle interruption signal to close database connection
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Println("Shutting down...")
			if db != nil {
				err = db.Close()
				if err != nil {
					log.Println(err)
				} else {
					log.Println("Database connection closed")
				}
			}
			os.Exit(0)
		}
	}()

	log.Println("Init worker server")
	user := os.Getenv("DB_USER")
	passwd := os.Getenv("DB_PASSWORD")
	protocol := "tcp"
	addr := os.Getenv("DB_ADDR")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	// check if env variables are set
	if user == "" || passwd == "" || addr == "" || port == "" || name == "" {
		log.Println("Env vars should be set. Using default values.")
		user = "user"
		passwd = "user"
		addr = "localhost"
		port = "3306"
		name = "dbname"
	}
	// create mysql config
	cfg := mysql.Config{
		User:                 user,
		Passwd:               passwd,
		Net:                  protocol,
		Addr:                 addr + ":" + port,
		DBName:               name,
		AllowNativePasswords: true,
	}
	// Try to open connection
	for {
		log.Println("Conf: ", cfg.FormatDSN())
		db, err = sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			log.Printf("Error opening database: %s, waiting %v seconds", err, WAIT_TIME)
			time.Sleep(WAIT_TIME * time.Second)
			continue
		}
		defer func() {
			if db == nil {
				return
			}
			err = db.Close()
			if err != nil {
				log.Println(err)
			} else {
				log.Println("Database connection closed")
			}
		}()
		log.Println("Database connection opened")
		break
	}

	// Get queue jobs and process them
	for {
		memory, err := memory.Get()
		if err != nil {
			log.Println(err)
			return
		}
		usedPercentage := float64(memory.Used) / float64(memory.Total) * 100
		// log.Printf("Porcentaje de memoria usada: %v %%\n", usedPercentage)
		if usedPercentage > MAX_MEMORY_USAGE {
			log.Printf("Memory usage is too high, waiting %d seconds", WAIT_TIME)
			time.Sleep(WAIT_TIME * time.Second)
			continue
		}

		// Get current unix time in seconds
		nowUnix := time.Now().Unix()
		// Update one undone job from database
		query := "UPDATE cola SET ultima_actualizacion = ? WHERE ultima_actualizacion + " +
			strconv.Itoa(MAX_PROCESSING_TIME) + " < " + strconv.FormatInt(nowUnix, 10) +
			" AND last_insert_id(id) LIMIT 1"
		stmt, _ := db.Prepare(query)

		result, err := stmt.Exec(nowUnix)
		if err != nil {
			log.Printf("Error executing statement: %s, waiting %d seconds", err, WAIT_TIME)
			time.Sleep(WAIT_TIME * time.Second)
			continue
		}
		stmt.Close()

		proceso_id, _ := result.LastInsertId()
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			log.Println("No jobs to process")
			time.Sleep(WAIT_TIME * time.Second)
			continue
		}

		log.Printf("Job %d selected. Affected %d", proceso_id, rowsAffected)

		fields := "id, simulacion_id, ultima_actualizacion, num_generaciones"
		query = "SELECT " + fields + " FROM cola WHERE id = ?"
		stmt, _ = db.Prepare(query)

		var jobToDo Cola
		err = stmt.QueryRow(proceso_id).Scan(&jobToDo.id, &jobToDo.simulacion_id, &jobToDo.ultima_actualizacion, &jobToDo.num_generaciones)
		if err != nil {
			log.Printf("Error executing query: %s, waiting %d seconds", err, WAIT_TIME)
			time.Sleep(WAIT_TIME * time.Second)
			continue
		}
		stmt.Close()

		fields = "id, usuario_id, nombre, descripcion, anchura, altura, estados, reglas, tipo"
		query = "SELECT " + fields + " FROM simulaciones WHERE id = ?"
		stmt, _ = db.Prepare(query)

		var simulation Simulaciones
		var simulationRules []ReglaSimul
		err = stmt.QueryRow(jobToDo.simulacion_id).Scan(&simulation.id, &simulation.usuario_id, &simulation.nombre, &simulation.descripcion, &simulation.anchura, &simulation.altura, &simulation.estados, &simulation.reglas, &simulation.tipo)
		if err != nil {
			log.Printf("Error executing query: %s, waiting %d seconds", err, WAIT_TIME)
			time.Sleep(WAIT_TIME * time.Second)
			continue
		}
		stmt.Close()

		err = json.Unmarshal([]byte(simulation.reglas), &simulationRules)
		if err != nil {
			log.Printf("Error unmarshalling json: %s, waiting %d seconds", err, WAIT_TIME)
			time.Sleep(WAIT_TIME * time.Second)
			continue
		}

		fields = "id, iteracion, simulacion_id, contenido"
		query = "SELECT " + fields + " FROM generaciones WHERE simulacion_id = ? ORDER BY iteracion DESC LIMIT 1"
		stmt, _ = db.Prepare(query)

		var lastGen Generaciones
		err = stmt.QueryRow(simulation.id).Scan(&lastGen.id, &lastGen.iteracion, &lastGen.simulacion_id, &lastGen.contenido)
		if err != nil {
			log.Printf("Error executing query: %s, waiting %d seconds", err, WAIT_TIME)
			time.Sleep(WAIT_TIME * time.Second)
			continue
		}
		stmt.Close()

		log.Println("Creating automaton")
		automaton := cella.NewCella2d(simulation.anchura, simulation.altura, simulation.estados)
		initGrid := cella.NewGrid(simulation.anchura, simulation.altura)
		nextGrid := cella.NewGrid(simulation.anchura, simulation.altura)
		loadContentToGrid(lastGen.contenido, initGrid)
		automaton.SetInitGrid(initGrid)
		automaton.SetNextGrid(nextGrid)
		automaton.SetAuxBordersAsToroidal()

		automatonRules := make([]*cella.Rule2d, len(simulationRules))
		for i, rule := range simulationRules {
			automatonRules[i] = cella.NewRule2d(rule.Condition, cella.Cell(rule.State), simulation.estados)
		}
		automaton.SetRules(automatonRules)

		log.Printf("Processing automaton sim: %v cola: %v", simulation.nombre, jobToDo.id)
		go processAutomaton(db, automaton, simulation, lastGen, jobToDo)
		time.Sleep(WAIT_TIME * time.Second)
	}
}
