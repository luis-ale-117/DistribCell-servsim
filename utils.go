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
	query := `UPDATE cola SET ultima_actualizacion = ? WHERE id = ?;`
	stmt, _ := db.Prepare(query)

	_, err := stmt.Exec(time.Now().Unix(), proceso.id)
	if err != nil {
		log.Fatalf("Error executing query: %s", err)
	}
	stmt.Close()
	for i := lastGen.iteracion; i < proceso.num_generaciones; i++ {
		log.Printf("Processing generation %d of %d for %s", i, proceso.num_generaciones, simulation.nombre)
		automaton.SetAuxBordersAsToroidal()
		if automaton.NextGeneration() != nil {
			log.Fatalf("Error processing generation %d for job %s", i, simulation.nombre)
		}

		log.Printf("Saving generation %d of %d to database for job %s", i+1, proceso.num_generaciones, simulation.nombre)
		content := automaton.GetNextGrid()
		query := "INSERT INTO generaciones (simulacion_id, iteracion, contenido) VALUES (?, ?, ?)"
		stmt, _ := db.Prepare(query)
		_, err = stmt.Exec(simulation.id, i+1, gridToBytes(content))
		if err != nil {
			log.Fatalf("Error executing query: %s", err)
		}
		stmt.Close()

		log.Printf("Updating job last update for %s cola %d simid %d", simulation.nombre, proceso.id, simulation.id)
		query = "UPDATE cola SET ultima_actualizacion = ? WHERE id = ?"
		stmt, _ = db.Prepare(query)
		_, err = stmt.Exec(time.Now().Unix(), proceso.id)
		if err != nil {
			log.Fatalf("Error executing query: %s", err)
		}
		stmt.Close()
		automaton.InitGrid, automaton.NextGrid = automaton.NextGrid, automaton.InitGrid
	}
	// Delete job from queue
	log.Printf("Deleting process from queue for %s", simulation.nombre)
	query = "DELETE FROM cola WHERE id = ?"
	stmt, _ = db.Prepare(query)
	_, err = stmt.Exec(proceso.id)
	if err != nil {
		log.Fatalf("Error executing query: %s", err)
	}
	stmt.Close()

	// Update tipo from PROCESAMIENTO to PROCESADO in simulation
	log.Printf("Updating simulation tipo to PROCESSED for %s %d", simulation.nombre, simulation.id)
	query = "UPDATE simulaciones SET tipo = 'PROCESADO' WHERE id = ?"
	stmt, _ = db.Prepare(query)
	_, err = stmt.Exec(simulation.id)
	if err != nil {
		log.Fatalf("Error executing query: %s", err)
	}
	stmt.Close()

	log.Println("Done")
}
