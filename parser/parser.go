package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
        "errors"
	"github.com/jackc/pglogrepl/protocol"

	"github.com/sirupsen/logrus"
)

// Variable with connection errors.
var (
	errReplConnectionIsLost = errors.New("replication connection to postgres is lost")
	errConnectionIsLost     = errors.New("db connection to postgres is lost")
	errMessageLost          = errors.New("messages are lost")
	errEmptyWALMessage      = errors.New("empty WAL message")
	errUnknownMessageType   = errors.New("unknown message type")
)

// BinaryParser represent binary protocol parser.
type BinaryParser struct {
	byteOrder binary.ByteOrder
	msgType   byte
	buffer    *bytes.Buffer
}

// NewBinaryParser create instance of binary parser.
func NewBinaryParser() *BinaryParser {
	return &BinaryParser{
		byteOrder: binary.BigEndian,
	}
}

// ParseWalMessage parse postgres WAL message.
// https://www.postgresql.org/docs/13/protocol-logicalrep-message-formats.html
func (p *BinaryParser) ParseWalMessage(msg []byte) error {
	if len(msg) == 0 {
		return errEmptyWALMessage
	}
	p.msgType = msg[0]
	p.buffer = bytes.NewBuffer(msg[1:])
	switch p.msgType {
	case protocol.BeginMsgType:
		fmt.Println("Begin: ", p.msgType)
		break
		begin := p.getBeginMsg()
		logrus.
			WithFields(
				logrus.Fields{
					"lsn": begin.LSN,
					"xid": begin.XID,
				}).
			Infoln("receive begin message")
	case protocol.CommitMsgType:
		fmt.Println("Commit: ", p.msgType)
		break
		commit := p.getCommitMsg()
		logrus.
			WithFields(
				logrus.Fields{
					"lsn":             commit.LSN,
					"transaction_lsn": commit.TransactionLSN,
				}).
			Infoln("receive commit message")
	case protocol.OriginMsgType:
		logrus.Infoln("receive origin message")
	case protocol.RelationMsgType:
		fmt.Println("Relation: ", p.msgType)
		break
		relation := p.getRelationMsg()
		logrus.
			WithFields(
				logrus.Fields{
					"relation_id": relation.ID,
					"replica":     relation.Replica,
				}).
			Infoln("receive relation message")
	case protocol.TypeMsgType:
		logrus.Infoln("type")
	case protocol.InsertMsgType:
		insert := p.getInsertMsg()
		logrus.
			WithFields(
				logrus.Fields{
					"relation_id": insert.RelationID,
				}).
			Infoln("receive insert message")
	case protocol.UpdateMsgType:
		fmt.Println("Update: ", p.msgType)
		upd := p.getUpdateMsg()
		logrus.
			WithFields(
				logrus.Fields{
					"relation_id": upd.RelationID,
				}).
			Infoln("receive update message")
		fmt.Println("Update: ", upd)
	case protocol.DeleteMsgType:
		del := p.getDeleteMsg()
		logrus.
			WithFields(
				logrus.Fields{
					"relation_id": del.RelationID,
				}).
			Infoln("receive delete message")
	default:
		return fmt.Errorf("%w : %s", errUnknownMessageType, []byte{p.msgType})
	}
	return nil
}

func (p *BinaryParser) getBeginMsg() protocol.Begin {
	return protocol.Begin{
		LSN:       p.readInt64(),
		Timestamp: p.readTimestamp(),
		XID:       p.readInt32(),
	}
}

func (p *BinaryParser) getCommitMsg() protocol.Commit {
	return protocol.Commit{
		Flags:          p.readInt8(),
		LSN:            p.readInt64(),
		TransactionLSN: p.readInt64(),
		Timestamp:      p.readTimestamp(),
	}
}

func (p *BinaryParser) getInsertMsg() protocol.Insert {
	return protocol.Insert{
		RelationID: p.readInt32(),
		NewTuple:   p.buffer.Next(1)[0] == protocol.NewTupleDataType,
		Row:        p.readTupleData(),
	}
}

func (p *BinaryParser) getDeleteMsg() protocol.Delete {
	return protocol.Delete{
		RelationID: p.readInt32(),
		KeyTuple:   p.charIsExists('K'),
		OldTuple:   p.charIsExists('O'),
		Row:        p.readTupleData(),
	}
}

func (p *BinaryParser) getUpdateMsg() protocol.Update {
	fmt.Println("getUpdateMsg:")
	u := protocol.Update{}
	u.RelationID = p.readInt32()
	u.KeyTuple = p.charIsExists('K')
	u.OldTuple = p.charIsExists('O')
	if u.KeyTuple || u.OldTuple {
		u.OldRow = p.readTupleData()
	}
	u.OldTuple = p.charIsExists('N')
	u.Row = p.readTupleData()
	return u
}

func (p *BinaryParser) getRelationMsg() protocol.Relation {
	return protocol.Relation{
		ID:        p.readInt32(),
		Namespace: p.readString(),
		Name:      p.readString(),
		Replica:   p.readInt8(),
		Columns:   p.readColumns(),
	}
}

func (p *BinaryParser) readInt32() (val int32) {
	r := bytes.NewReader(p.buffer.Next(4))
	_ = binary.Read(r, p.byteOrder, &val)
	return
}

func (p *BinaryParser) readInt64() (val int64) {
	r := bytes.NewReader(p.buffer.Next(8))
	_ = binary.Read(r, p.byteOrder, &val)
	return
}

func (p *BinaryParser) readInt8() (val int8) {
	r := bytes.NewReader(p.buffer.Next(1))
	_ = binary.Read(r, p.byteOrder, &val)
	return
}

func (p *BinaryParser) readInt16() (val int16) {
	r := bytes.NewReader(p.buffer.Next(2))
	_ = binary.Read(r, p.byteOrder, &val)
	return
}

func (p *BinaryParser) readTimestamp() time.Time {
	ns := p.readInt64()
	return protocol.PostgresEpoch.Add(time.Duration(ns) * time.Microsecond)
}

func (p *BinaryParser) readString() (str string) {
	stringBytes, _ := p.buffer.ReadBytes(0)
	return string(bytes.Trim(stringBytes, "\x00"))
}

func (p *BinaryParser) readBool() bool {
	x := p.buffer.Next(1)[0]
	return x != 0
}

func (p *BinaryParser) charIsExists(char byte) bool {
	x := p.buffer.Next(1)[0]
	fmt.Println("x: ", x)
	if x == char {
		return true
	}
	_ = p.buffer.UnreadByte()
	return false
}

func (p *BinaryParser) readColumns() []protocol.RelationColumn {
	size := int(p.readInt16())
	data := make([]protocol.RelationColumn, size)
	for i := 0; i < size; i++ {
		data[i] = protocol.RelationColumn{
			Key:          p.readBool(),
			Name:         p.readString(),
			TypeID:       p.readInt32(),
			ModifierType: p.readInt32(),
		}
	}
	return data
}

func (p *BinaryParser) readTupleData() []protocol.TupleData {
	fmt.Println("readTupleData:")
	size := 4 // int(p.readInt16())
	data := make([]protocol.TupleData, size)
	for i := 0; i < size; i++ {
		sl := p.buffer.Next(1)
		fmt.Println("%v", sl)
		switch sl[0] {
		case protocol.NullDataType:
			logrus.Debugln("tupleData: null data type")
		case protocol.ToastDataType:
			logrus.Debugln(
				"tupleData: toast data type")
		case protocol.TextDataType:
			vsize := int(p.readInt8())
			data[i] = protocol.TupleData{Value: p.buffer.Next(vsize)}
			fmt.Println("text: ", data[i])
		}
	}
	return data
}
