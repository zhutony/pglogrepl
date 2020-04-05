package main

import (
	"context"
	"log"
	"os"
	"time"
	"text/template"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
)

func main() {
	//	const outputPlugin = "test_decoding"
	const outputPlugin = "pgoutput"
	conn, err := pgconn.Connect(context.Background(), os.Getenv("PGLOGREPL_MBIP_CONN_STRING"))
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}
	defer conn.Close(context.Background())

	var pluginArguments []string
	if outputPlugin == "pgoutput" {
		result := conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS pglogrepl_mbip;")
		_, err := result.ReadAll()
		if err != nil {
			log.Fatalln("drop publication if exists error", err)
		}

		result = conn.Exec(context.Background(), "CREATE PUBLICATION pglogrepl_mbip FOR ALL TABLES;")
		_, err = result.ReadAll()
		if err != nil {
			log.Fatalln("create publication error", err)
		}
		log.Println("create publication pglogrepl_mbip")

		pluginArguments = []string{"proto_version '1'", "publication_names 'pglogrepl_mbip'"}
	}

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	slotName := "pglogrepl_mbip"

	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		log.Fatalln("CreateReplicationSlot failed:", err)
	}
	log.Println("Created temporary replication slot:", slotName)
	err = pglogrepl.StartReplication(context.Background(), conn, slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		log.Fatalln("StartReplication failed:", err)
	}
	log.Println("Logical replication started on slot", slotName)

	clientXLogPos := sysident.XLogPos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			log.Println("Sent Standby status message")
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		msg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Fatalln("ReceiveMessage failed:", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
				}
				log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParseXLogData failed:", err)
				}
				log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", string(xld.WALData))
				type Socket struct {
					IP string
					Port uint
				}
			       	members := []Socket{{"wool", 17}}
			       	tmpl, err := template.New("test").Parse(`pool pool1 {
					{{range .}}member {{.IP}} {{.Port}}\n{{end}}
									 }`)
			       	if err != nil { panic(err) }
			       	err = tmpl.Execute(os.Stdout, members)
			       	if err != nil { panic(err) }

				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		default:
			log.Printf("Received unexpected message: %#v\n", msg)
		}

	}
}
