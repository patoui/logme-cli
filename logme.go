package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/joho/godotenv"
	"github.com/urfave/cli/v2"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	app := &cli.App{
		Name:  "logme-cli",
		Usage: "A tool to help with commands for LogMe app!",
		Commands: []*cli.Command{
			{
				Name:    "migrate",
				Aliases: []string{"m"},
				Usage:   "migrate the database",
				Description: `
				This command will migrate the database while using the environment variables (.env or otherwise):
					DB_LOCAL_ADDR - includes host and port
					DB_ADDR - includes host and port
					DB_NAME - name of the database to migrate (defaults to 'logme')
					DB_USER (optional) - user to authenticate with
					DB_PASS (optional) - password to authenticate with
				`,
				Action: func(c *cli.Context) error {
					return migrate(false)
				},
			},
			{
				Name:    "migrate-test",
				Aliases: []string{"mt"},
				Usage:   "migrate the test database",
				Description: `
				This command will migrate the database while using the environment variables (.env or otherwise):
					DB_LOCAL_ADDR - includes host and port
					DB_ADDR - includes host and port
					DB_NAME - name of the database to migrate (defaults to 'logme'), '_test' will automatically be appended
					DB_USER (optional) - user to authenticate with
					DB_PASS (optional) - password to authenticate with
				`,
				Action: func(c *cli.Context) error {
					return migrate(true)
				},
			},
			{
				Name:    "up",
				Aliases: []string{"u"},
				Usage:   "start logme docker containers",
				Description: `Start logme containers`,
				Action: func(c *cli.Context) error {
					return up()
				},
			},
			{
				Name:    "down",
				Aliases: []string{"d"},
				Usage:   "stop logme docker containers",
				Description: `Stop logme containers`,
				Action: func(c *cli.Context) error {
					return down()
				},
			},
			{
				Name:    "list",
				Aliases: []string{"l"},
				Usage:   "list logme docker containers",
				Description: `List logme docker containers`,
				Action: func(c *cli.Context) error {
					return list()
				},
			},
			{
				Name:    "test",
				Aliases: []string{"t"},
				Usage:   "run logme test",
				Description: `Run logme tests`,
				Action: func(c *cli.Context) error {
					return test()
				},
			},
		},
	}

	sort.Sort(cli.CommandsByName(app.Commands))

	err = app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func migrate(isTest bool) error {
	db, err := getDbConn(isTest)
	if err != nil {
		return err
	}
	if err := createMigrationsTable(db); err != nil {
		return err
	}
	return runMigrations(db)
}

func getDbConn(isTest bool) (driver.Conn, error) {
	localAddr := os.Getenv("DB_LOCAL_ADDR")
	addr := os.Getenv("DB_ADDR")

	if addr == "" {
		addr = localAddr
	}

	if addr == "" {
		return nil, errors.New("environment variable DB_ADDR or DB_LOCAL_ADDR required for migrations")
	}

	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		dbName = "logme"
	}

	dbSuffix := ""
	if isTest {
		dbSuffix = "_test"
	}

	auth := clickhouse.Auth{
		Database: dbName + dbSuffix,
	}

	if user := os.Getenv("DB_USER"); user != "" {
		auth.Username = user
	}

	if pass := os.Getenv("DB_PASS"); pass != "" {
		auth.Password = pass
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: auth,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
	})

	// Failed to connect
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func createMigrationsTable(db driver.Conn) error {
	sqlExists := "SHOW TABLES LIKE 'migrations'"

	var exists string
	if err := db.QueryRow(context.Background(), sqlExists).Scan(&exists); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			// unknown error
			panic(err.Error())
		}
	}

	// migrations table already exists
	if exists != "" {
		return nil
	}

	err := db.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS migrations (
			name       String,
			dt         DateTime
		) engine=MergeTree() ORDER BY (name, dt)
	`)

	if err != nil {
		return err
	}

	return nil
}

func runMigrations(db driver.Conn) error {
	migrationDir := "internal/logme/migrations/"
	files, err := ioutil.ReadDir(migrationDir)
	if err != nil {
		return err
	}

	ctx := context.Background()

	for _, file := range files {
		// skip directories
		if file.IsDir() {
			continue
		}

		// skip non-sql files
		if !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}

		sqlExists := fmt.Sprintf("SELECT 1 FROM migrations WHERE name = '%s'", file.Name())

		var exists uint8
		if err := db.QueryRow(ctx, sqlExists).Scan(&exists); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				// unknown error
				return err
			}
		}

		// migration already ran, continue
		if exists == 1 {
			continue
		}

		content, err := os.ReadFile(migrationDir + file.Name())
		if err != nil {
			return err
		}

		err = db.Exec(ctx, string(content))

		if err != nil {
			return err
		}

		err = db.AsyncInsert(
			ctx,
			fmt.Sprintf(
				`INSERT INTO migrations (name, dt) VALUES ('%s', %d)`,
				file.Name(),
				time.Now().Unix(),
			),
			false,
		)

		if err != nil {
			return err
		}

		fmt.Println("Successfully migrated: " + file.Name())
	}

	return nil
}

func up() error {
	out, err := exec.Command("/bin/sh", "-c", "docker-compose up", "-d").Output()

	if (err != nil) {
		return err
	}

	fmt.Printf("%s\n", out)

	return nil
}

func down() error {
	out, err := exec.Command("/bin/sh", "-c", "docker-compose down").Output()

	if (err != nil) {
		return err
	}

	fmt.Printf("%s\n", out)

	return nil
}

func list() error {
	out, err := exec.Command("/bin/sh", "-c", "docker ps --format \"table {{.ID}}\t{{.Names}}\t{{.State}}\t{{.Ports}}\"").Output()

	if (err != nil) {
		return err
	}

	fmt.Printf("%s\n", out)

	return nil
}

func test() error {
	out, _ := exec.Command("/bin/sh", "-c", "docker exec -i logme_server /usr/local/go/bin/go test").Output()

	fmt.Printf("%s\n", out)

	return nil
}

