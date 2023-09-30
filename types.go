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
