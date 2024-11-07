package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/gocql/gocql"
	"github.com/segmentio/kafka-go"

	"os"
	"os/exec"
)

// Column represents a column in the table
type Column struct {
	Name       string `json:"name"`
	DataType   string `json:"data_type"`
	IsNullable bool   `json:"is_nullable"`
	IsPrimary  bool   `json:"is_primary"`
}

// Table represents a table in the schema
type Table struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

// Schema represents the entire schema with multiple tables
type Schema struct {
	DatabaseName string  `json:"database_name"`
	Tables       []Table `json:"tables"`
}

// Convert MySQL data types to Hive data types
func mysqlToHiveDataType(mysqlType string) string {
	switch mysqlType {
	case "int", "smallint", "mediumint", "bigint":
		return "INT"
	case "varchar", "char", "text":
		return "STRING"
	case "float", "double", "decimal":
		return "DOUBLE"
	case "date", "datetime", "timestamp":
		return "TIMESTAMP"
	default:
		return "STRING" // Default to STRING for unsupported types
	}
}

// Generate Hive database creation script
func generateHiveDatabaseSQL(databaseName string) string {
	return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", databaseName)
}

// Generate Hive table creation script
func generateHiveTableSQL(databaseName string, table Table) string {
	var sql strings.Builder
	sql.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (\n", databaseName, table.Name))
	for _, col := range table.Columns {
		// Convert MySQL data type to Hive data type
		hiveDataType := mysqlToHiveDataType(col.DataType)
		sql.WriteString(fmt.Sprintf("  %s %s", col.Name, hiveDataType))
		if !col.IsNullable {
			sql.WriteString(" NOT NULL")
		}
		sql.WriteString(",\n")
	}
	// Remove the last comma and newline
	sqlString := sql.String()
	sqlString = sqlString[:len(sqlString)-2] + "\n);"
	return sqlString
}

func main() {

	cluster := gocql.NewCluster("127.0.0.1")
	session, err := cluster.CreateSession()

	if err != nil {
		log.Fatal(err)
	}

	defer session.Close()

	kafkaReader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "variant",
			GroupID: "0",
		})

	for {
		message, err := kafkaReader.ReadMessage(context.Background())

		if err != nil {
			log.Fatal(err)
		}

		// Deserialize schema from JSON
		var schema Schema
		err = json.Unmarshal(message.Value, &schema)
		if err != nil {
			log.Fatal("Error unmarshalling schema:", err)
		}

		// Generate Hive database creation script
		databaseSQL := generateHiveDatabaseSQL(schema.DatabaseName)
		errs := executeHiveSQL(databaseSQL)

		if errs != nil {
			log.Fatalf("Error creating database in Hive: %v", errs)
		}

		// Loop through tables in the schema and generate Hive table creation scripts
		for _, table := range schema.Tables {
			tableSQL := generateHiveTableSQL(schema.DatabaseName, table)
			errs = executeHiveSQL(tableSQL)

			if errs != nil {
				log.Fatalf("Error creating table in Hive: %v", errs)
			}
		}

	}
}

// Placeholder for executing Hive SQL statement (replace this with actual execution logic)
func executeHiveSQL(sql string) error {
	fmt.Println(sql)

	env := append(os.Environ(), "HADOOP_HOME=/usr/local/hadoop", "PATH=/usr/local/hadoop/bin:"+os.Getenv("PATH"))
	// Run the 'hive' command with the updated PATH
	cmd := exec.Command("/usr/local/hive/bin/hive", "-e", sql)
	cmd.Env = env
	// cmd := exec.Command("beeline", "-u", "jdbc:hive2://localhost:10000/default", "-e", sql)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to execute Hive query: %v", err)
	}
	return nil

}
