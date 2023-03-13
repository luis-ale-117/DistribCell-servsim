package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/go-sql-driver/mysql"
)

func main() {
	log.Println("Init worker server")
	user := os.Getenv("DB_USER")
	passwd := os.Getenv("DB_PASSWORD")
	protocol := "tcp"
	addr := os.Getenv("DB_ADDR")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	cfg := mysql.Config{
		User:                 user,
		Passwd:               passwd,
		Net:                  protocol,
		Addr:                 fmt.Sprintf("%s:%s", addr, port),
		DBName:               name,
		AllowNativePasswords: true,
	}
	fmt.Println(cfg)
	db, err := sql.Open("mysql", cfg.FormatDSN())
	defer func() {
		if db != nil {
			err = db.Close()
			if err != nil {
				log.Println(err)
			}
		}
	}()

	if err != nil {
		log.Fatal(err)
	}
	pingErr := db.Ping()
	if pingErr != nil {
		log.Fatal(pingErr)
	}
	fmt.Println("Connected!!")

}
