cd ./example/pglogrepl_demo/
export PGLOGREPL_MBIP_CONN_STRING="postgres://postgres:postgres@127.0.0.1/mbip?replication=database" 
go run main.go
