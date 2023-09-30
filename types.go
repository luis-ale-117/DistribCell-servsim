package main

import "database/sql"

type Job struct {
	id            int
	simulacion_id int
	// proceso_id           sql.NullString
	// estado               string
	num_generaciones     int
	ultima_actualizacion int64
}

type Simulation struct {
	id          int
	usuario_id  int
	nombre      string
	descripcion sql.NullString
	anchura     int
	altura      int
	estados     int
	reglas      string
	tipo        string
}

type SimulationRule struct {
	Condition string `json:"condition"`
	State     int    `json:"state"`
}

type Generation struct {
	id            int
	iteracion     int
	simulacion_id int
	contenido     []byte
}

func matrixFromContent(content []byte, width, height int) [][]byte {
	matrix := make([][]byte, height)
	for i := range matrix {
		matrix[i] = make([]byte, width)
	}
	for i := 0; i < len(content); i++ {
		matrix[i/height][i%width] = content[i]
	}
	return matrix
}
