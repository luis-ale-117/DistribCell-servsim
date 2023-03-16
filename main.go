package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	WAIT_TIME = 5 // seconds
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
		Addr:                 fmt.Sprintf("%s:%s", addr, port),
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
		log.Println("Database connection opened")
		break
	}

	// Ping database
	for {
		err = db.Ping()
		if err != nil {
			log.Printf("Error pinging database: %s, waiting %v seconds", err, WAIT_TIME)
			time.Sleep(WAIT_TIME * time.Second)
			continue
		}
		log.Println("Database connection established")
		break
	}

	// Do something with database
	for {
		// time now
		now := time.Now()
		// unix
		unix := now.Unix()
		log.Println(unix)

		// update query
		// UPDATE Proyectos SET (proceso_id = ?, ulrima_actualizacion=ahora) WHERE estado!='DONE' AND ahora-ultima_actualizacion > tiempo_limite ORDER BY ultima_actualizacion ASC LIMIT 1;
		query := "UPDATE `Proyectos` SET `proceso_id` = ?,`estado` = ?, `ultima_actualizacion`=? WHERE `estado` != ?" +
			" AND ? - `ultima_actualizacion` > ? ORDER BY `ultima_actualizacion` ASC LIMIT 1"
		// prepare statement
		stmt, err := db.Prepare(query)
		if err != nil {
			log.Println(err)
			time.Sleep(1 * time.Second)
			continue
		}
		// execute statement
		_, err = stmt.Exec(unix, "PROCESSING", unix, "DONE", unix, 5)
		if err != nil {
			log.Println(err)
			time.Sleep(1 * time.Second)
			continue
		}
		// close statement
		err = stmt.Close()
		if err != nil {
			log.Println(err)
			time.Sleep(1 * time.Second)
			continue
		}

		// select query
		query = "SELECT * FROM `Proyectos` WHERE `proceso_id` = ? ORDER BY `ultima_actualizacion` ASC LIMIT 1"

		// prepare statement
		stmt, err = db.Prepare(query)
		if err != nil {
			log.Println(err)
			time.Sleep(1 * time.Second)
			continue
		}
		// execute statement
		rows, err := stmt.Query(unix)
		if err != nil {
			log.Println(err)
			time.Sleep(1 * time.Second)
			continue
		}
		// Print rows
		for rows.Next() {
			var id int
			var usuario_id int
			var name string
			// Consider description can be null
			var descripcion sql.NullString
			var estado string
			var ultima_actualizacion int64
			var proceso_id int64

			err = rows.Scan(&id, &usuario_id, &name, &descripcion, &estado, &ultima_actualizacion, &proceso_id)
			if err != nil {
				log.Println(err)
				time.Sleep(1 * time.Second)
				continue
			}
			log.Printf("id: %d, usuario_id: %d, name: %s, descripcion: %s, estado: %s, ultima_actualizacion: %d, proceso_id: %d",
				id, usuario_id, name, descripcion.String, estado, ultima_actualizacion, proceso_id)

		}
		// close statement
		err = stmt.Close()
		if err != nil {
			log.Println(err)
			time.Sleep(1 * time.Second)
			continue
		}

		// Do something
		log.Println("Doing something")
		time.Sleep(1 * time.Second)
	}

}
