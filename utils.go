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

func processAutomaton(db *sql.DB, automaton *cella.Cella2d, simulation Simulation, lastGen Generation, job Job) {
	// Do something
	log.Println("Doing something")
	time.Sleep(WAIT_TIME * time.Second)
}
