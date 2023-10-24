package main

import (
	"database/sql"
)

type Cola struct {
	id                   int   `db:"id"`
	simulacion_id        int   `db:"simulacion_id"`
	num_generaciones     int   `db:"num_generaciones"`
	ultima_actualizacion int64 `db:"ultima_actualizacion"`
}

type Simulaciones struct {
	id          int            `db:"id"`
	usuario_id  int            `db:"usuario_id"`
	nombre      string         `db:"nombre"`
	descripcion sql.NullString `db:"descripcion"`
	anchura     int            `db:"anchura"`
	altura      int            `db:"altura"`
	estados     int            `db:"estados"`
	reglas      string         `db:"reglas"`
	tipo        string         `db:"tipo"`
}

type ReglaSimul struct {
	Condition string `json:"condition"`
	State     int    `json:"state"`
}

type Generaciones struct {
	id            int    `db:"id"`
	iteracion     int    `db:"iteracion"`
	simulacion_id int    `db:"simulacion_id"`
	contenido     []byte `db:"contenido"`
}
