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
	MAX_PROCESSING_TIME = 20 // seconds
	MAX_MEMORY_USAGE    = 70 // percentage
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
		log.Fatal("DB_USER, DB_PASSWORD, DB_ADDR, DB_PORT, DB_NAME must be set")
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
		db, err = sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			log.Printf("Error opening database: %s, waiting %v seconds", err, WAIT_TIME)
			time.Sleep(WAIT_TIME * time.Second)
			continue
		}
		defer func() {
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
		log.Printf("memory total: %d Mbytes\n", memory.Total/1024/1024)
		log.Printf("memory used: %d Mbytes\n", memory.Used/1024/1024)
		log.Printf("Porcentaje de memoria usada: %v %%\n", usedPercentage)
		if usedPercentage > MAX_MEMORY_USAGE {
			log.Printf("Memory usage is too high, waiting %d seconds", WAIT_TIME)
			time.Sleep(WAIT_TIME * time.Second)
			continue
		}

		// Get current unix time in seconds
		nowUnix := time.Now().Unix()
		// Update one undone job from database
		query := "UPDATE `cola` SET ultima_actualizacion=? WHERE `ultima_actualizacion` + " +
			strconv.Itoa(MAX_PROCESSING_TIME) + "<" + strconv.FormatInt(nowUnix, 10) +
			" AND last_insert_id(id) ORDER BY `ultima_actualizacion` ASC LIMIT 1"
		stmt, err := db.Prepare(query)
		if err != nil {
			log.Println(err)
			time.Sleep(1 * time.Second)
			continue
		}

		result, err := stmt.Exec(nowUnix)
		if err != nil {
			log.Printf("Error executing statement: %s, waiting %d seconds", err, WAIT_TIME)
			time.Sleep(WAIT_TIME * time.Second)
			continue
		}
		job_id, _ := result.LastInsertId()
		rowsAffected, _ := result.RowsAffected()
		log.Println("Process id: ", job_id, " Rows affected: ", rowsAffected)
		stmt.Close()

		if rowsAffected == 0 {
			log.Println("No jobs to process")
			time.Sleep(WAIT_TIME * time.Second)
			continue
		}

		query = "SELECT * FROM `cola` WHERE `id` = ?"
		stmt, err = db.Prepare(query)
		if err != nil {
			log.Println(err)
			time.Sleep(1 * time.Second)
			continue
		}

		var jobToDo Job
		err = stmt.QueryRow(job_id).Scan(&jobToDo.id, &jobToDo.simulacion_id, &jobToDo.num_generaciones, &jobToDo.ultima_actualizacion)
		if err != nil {
			log.Printf("Error executing query: %s, waiting %d seconds", err, WAIT_TIME)
			time.Sleep(WAIT_TIME * time.Second)
			continue
		}
		stmt.Close()

		query = "SELECT * FROM `simulaciones` WHERE `id` = ?"
		stmt, err = db.Prepare(query)
		if err != nil {
			log.Println(err)
			time.Sleep(1 * time.Second)
			continue
		}

		var simulation Simulation
		var simulationRules []SimulationRule
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

		query = "SELECT * FROM `generaciones` WHERE `simulacion_id` = ? ORDER BY `iteracion` DESC LIMIT 1"
		stmt, err = db.Prepare(query)
		if err != nil {
			log.Println(err)
			time.Sleep(1 * time.Second)
			continue
		}

		var lastGen Generation
		err = stmt.QueryRow(simulation.id).Scan(&lastGen.id, &lastGen.simulacion_id, &lastGen.iteracion, &lastGen.contenido)
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

		log.Println("Processing automaton")
		go processAutomaton(db, automaton, simulation, lastGen, jobToDo)
	}

}
