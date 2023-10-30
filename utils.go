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

func processAutomaton(db *sql.DB, automaton *cella.Cella2d, simulation Simulaciones, lastGen Generaciones, proceso Cola) {
	log.Printf("Total: %d Done: %d for %s", proceso.num_generaciones, lastGen.iteracion, simulation.nombre)
	log.Printf("Updating job last update for %s cola %d simid %d", simulation.nombre, proceso.id, simulation.id)
	processError := false
	query := "UPDATE cola SET ultima_actualizacion = ? WHERE id = ?"
	_, err := db.Exec(query, time.Now().Unix(), proceso.id)
	if err != nil {
		log.Fatalf("Error executing query: %s", err)
	}

	for i := lastGen.iteracion; i < proceso.num_generaciones; i++ {
		log.Printf("Processing generation %d of %d for %s", i, proceso.num_generaciones, simulation.nombre)
		automaton.SetAuxBordersAsToroidal()
		if automaton.NextGeneration() != nil {
			log.Printf("Error processing generation %d for job %s", i, simulation.nombre)
			processError = true
			break
		}

		log.Printf("Saving generation %d of %d to database for job %s", i+1, proceso.num_generaciones, simulation.nombre)
		content := automaton.GetNextGrid()
		query := "INSERT INTO generaciones (simulacion_id, iteracion, contenido) VALUES (?, ?, ?)"
		_, err = db.Exec(query, i+1, gridToBytes(content))
		if err != nil {
			log.Fatalf("Error executing query: %s", err)
		}

		log.Printf("Updating job last update for %s cola %d simid %d", simulation.nombre, proceso.id, simulation.id)
		query = "UPDATE cola SET ultima_actualizacion = ? WHERE id = ?"
		_, err = db.Exec(query, time.Now().Unix(), proceso.id)
		if err != nil {
			log.Fatalf("Error executing query: %s", err)
		}
		automaton.InitGrid, automaton.NextGrid = automaton.NextGrid, automaton.InitGrid
	}

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Error executing query: %s", err)
	}
	defer tx.Rollback()

	// Delete job from queue
	log.Printf("Deleting process from queue for %s", simulation.nombre)
	query = "DELETE FROM cola WHERE id = ?"
	_, err = tx.Exec(query, proceso.id)
	if err != nil {
		log.Fatalf("Error executing query: %s", err)
	}

	// Update tipo from PROCESAMIENTO to PROCESADO in simulation
	tipo := "PROCESADO"
	if processError {
		tipo = "ERROR"
	}
	log.Printf("Updating simulation tipo to %s for %s %d", tipo, simulation.nombre, simulation.id)
	query = "UPDATE simulaciones SET tipo = '" + tipo + "' WHERE id = ?"
	_, err = tx.Exec(query, simulation.id)
	if err != nil {
		log.Fatalf("Error executing query: %s", err)
	}

	log.Println("Done")
	if err = tx.Commit(); err != nil {
		log.Fatalf("Error executing query: %s", err)
	}
}
