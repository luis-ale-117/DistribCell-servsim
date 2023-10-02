package main

import (
	"database/sql"
	"log"
	"time"

	"github.com/luis-ale-117/cella"
)

func loadContentToGrid(content []byte, grid *cella.Grid) {
	for i := 0; i < len(content); i++ {
		grid.SetCell(i%grid.Width, i/grid.Height, cella.Cell(content[i]))
	}
}

func gridToBytes(grid *cella.Grid) []byte {
	content := make([]byte, grid.Width*grid.Height)
	for i := 0; i < len(content); i++ {
		content[i] = byte(grid.GetCell(i%grid.Width, i/grid.Height))
	}
	return content
}

func processAutomaton(db *sql.DB, automaton *cella.Cella2d, simulation Simulation, lastGen Generation, job Job) {
	log.Println("Number of generations to process:", job.num_generaciones)
	log.Println("Last generation processed:", lastGen.iteracion)
	for i := lastGen.iteracion; i < job.num_generaciones; i++ {
		log.Printf("Processing generation %d for job %d", i, job.id)
		if automaton.NextGeneration() != nil {
			log.Fatalf("Error processing generation %d for job %d", i, job.id)
		}

		log.Println("Saving generation to database")
		content := automaton.GetNextGrid()
		query := "INSERT INTO `generaciones` (`simulacion_id`, `iteracion`, `contenido`) VALUES (?, ?, ?)"
		stmt, err := db.Prepare(query)
		if err != nil {
			log.Fatal(err)
		}
		_, err = stmt.Exec(simulation.id, i+1, gridToBytes(content))
		if err != nil {
			log.Fatalf("Error executing query: %s", err)
		}
		stmt.Close()

		log.Println("Updating job last update")
		query = "UPDATE `cola` SET `ultima_actualizacion` = ? WHERE `id` = ?"
		stmt, err = db.Prepare(query)
		if err != nil {
			log.Fatal(err)
		}
		_, err = stmt.Exec(time.Now().Unix(), job.id)
		if err != nil {
			log.Fatalf("Error executing query: %s", err)
		}
		stmt.Close()
		automaton.InitGrid, automaton.NextGrid = automaton.NextGrid, automaton.InitGrid
	}
	// Delete job from queue
	log.Println("Deleting job from queue")
	query := "DELETE FROM `cola` WHERE `id` = ?"
	stmt, err := db.Prepare(query)
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec(job.id)
	if err != nil {
		log.Fatalf("Error executing query: %s", err)
	}
	stmt.Close()

	// Update tipo from PROCESAMIENTO to PROCESADO in simulation
	log.Println("Updating simulation tipo to PROCESSED")
	query = "UPDATE `simulaciones` SET `tipo` = 'PROCESADO' WHERE `id` = ?"
	stmt, err = db.Prepare(query)
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec(simulation.id)
	if err != nil {
		log.Fatalf("Error executing query: %s", err)
	}
	stmt.Close()

	log.Println("Done")
}
