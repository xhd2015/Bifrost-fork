package mongo

import (
	"fmt"
	"testing"
	"time"

	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/rwynn/gtm/v2"
	"github.com/xhd2015/xgo/runtime/mock"
	"go.mongodb.org/mongo-driver/bson/primitive"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMongoInput_ToInputCallback(t *testing.T) {
	Convey("callback is nil", t, func() {
		c := new(MongoInput)
		c.SetCallback(nil)
		c.ToInputCallback(nil)
	})

	Convey("command ,not drop database,not drop table", t, func() {
		c := new(MongoInput)
		var callbackData *outputDriver.PluginDataType
		var callback = func(data *outputDriver.PluginDataType) {
			return
		}
		c.SetCallback(callback)
		op := &gtm.Op{
			Operation: "c",
		}
		mock.Patch(op.IsCommand, func() bool {
			return true
		})
		c.ToInputCallback(op)
		So(callbackData, ShouldBeNil)
	})

	Convey("command ,drop database", t, func() {
		c := new(MongoInput)
		var callbackData *outputDriver.PluginDataType
		var callback = func(data *outputDriver.PluginDataType) {
			callbackData = data
			return
		}
		c.SetCallback(callback)
		op := &gtm.Op{
			Operation: "c",
			Data:      map[string]interface{}{"dropDatabase": "database"},
		}
		mock.Patch(op.IsCommand, func() bool {
			return true
		})
		mock.Patch(c.BuildDropDatabaseQueryEvent, func(op *gtm.Op) *outputDriver.PluginDataType {
			return &outputDriver.PluginDataType{SchemaName: "database"}
		})

		c.ToInputCallback(op)
		So(callbackData, ShouldNotBeNil)
		So(callbackData.SchemaName, ShouldEqual, "database")
	})

	Convey("command ,not database, drop table", t, func() {
		c := new(MongoInput)
		var callbackData *outputDriver.PluginDataType
		var callback = func(data *outputDriver.PluginDataType) {
			callbackData = data
			return
		}
		c.SetCallback(callback)
		op := &gtm.Op{
			Operation: "c",
			Data:      map[string]interface{}{"drop": "testTableName"},
		}
		mock.Patch(op.IsCommand, func() bool {
			return true
		})
		mock.Patch(c.BuildDropTableQueryEvent, func(op *gtm.Op) *outputDriver.PluginDataType {
			return &outputDriver.PluginDataType{TableName: "testTableName", EventType: "sql"}
		})

		c.ToInputCallback(op)
		So(callbackData, ShouldNotBeNil)
		So(callbackData.TableName, ShouldEqual, "testTableName")
	})

	Convey("row event,transfer data nil ,callback is nil", t, func() {
		c := new(MongoInput)
		var callbackData *outputDriver.PluginDataType
		var callback = func(data *outputDriver.PluginDataType) {
			callbackData = data
			return
		}
		c.SetCallback(callback)
		op := &gtm.Op{
			Operation: "i",
		}
		mock.Patch(c.BuildRowEvent, func(op *gtm.Op) *outputDriver.PluginDataType {
			return nil
		})

		c.ToInputCallback(op)
		So(callbackData, ShouldBeNil)
	})

	Convey("insert row event", t, func() {
		c := new(MongoInput)
		var callbackData *outputDriver.PluginDataType
		var callback = func(data *outputDriver.PluginDataType) {
			callbackData = data
			return
		}
		c.SetCallback(callback)
		op := &gtm.Op{
			Operation: "i",
		}
		mock.Patch(c.BuildRowEvent, func(op *gtm.Op) *outputDriver.PluginDataType {
			return &outputDriver.PluginDataType{TableName: "testTableName", EventType: "insert"}
		})

		c.ToInputCallback(op)
		So(callbackData, ShouldNotBeNil)
		So(callbackData.TableName, ShouldEqual, "testTableName")
	})

	Convey("insert and commit event,callback not be nil", t, func() {
		c := new(MongoInput)
		var callbackData *outputDriver.PluginDataType
		var callback = func(data *outputDriver.PluginDataType) {
			callbackData = data
			return
		}
		c.SetCallback(callback)
		op := &gtm.Op{
			Operation: "i",
		}
		mock.Patch(c.BuildRowEvent, func(op *gtm.Op) *outputDriver.PluginDataType {
			return &outputDriver.PluginDataType{TableName: "testTableName", EventType: "insert"}
		})
		mock.Patch(c.BuildCommitEvent, func(data *outputDriver.PluginDataType) *outputDriver.PluginDataType {
			return &outputDriver.PluginDataType{TableName: "testTableName", EventType: "commit"}
		})

		c.ToInputCallback(op)
		So(callbackData, ShouldNotBeNil)
		So(callbackData.TableName, ShouldEqual, "testTableName")
		So(callbackData.EventType, ShouldEqual, "commit")
	})
}

func TestMongoInput_BuildRowEvent(t *testing.T) {
	Convey("op.Id not be primitive.ObjectID", t, func() {
		c := new(MongoInput)
		op := &gtm.Op{
			Operation: "i",
			Id:        "",
			Namespace: "database.table",
		}
		data := c.BuildRowEvent(op)
		So(data, ShouldBeNil)
	})

	Convey("op.Operation not be i,u,d", t, func() {
		c := new(MongoInput)
		op := &gtm.Op{
			Operation: "x",
			Id:        primitive.NewObjectID(),
			Namespace: "database.table",
			Data:      make(map[string]interface{}),
		}
		data := c.BuildRowEvent(op)
		So(data, ShouldBeNil)
	})

	Convey("op.Operation i", t, func() {
		c := new(MongoInput)
		op := &gtm.Op{
			Operation: "i",
			Id:        primitive.NewObjectID(),
			Namespace: "database.table",
			Data:      make(map[string]interface{}),
		}

		mock.Patch(c.OpLogPosition2GTID, func(p *primitive.Timestamp) string {
			return ""
		})
		mock.Patch(c.TransferDataAndColumnMapping, func(row map[string]interface{}) (columnMapping map[string]string) {
			return
		})
		data := c.BuildRowEvent(op)
		So(data, ShouldNotBeNil)
		So(data.SchemaName, ShouldEqual, "database")
		So(data.EventType, ShouldEqual, "insert")
		So(data.Rows[0]["_id"], ShouldEqual, op.Id.(primitive.ObjectID).Hex())
	})

	Convey("op.Operation u normal", t, func() {
		c := new(MongoInput)
		op := &gtm.Op{
			Operation: "u",
			Id:        primitive.NewObjectID(),
			Namespace: "database.table",
			Data:      map[string]interface{}{"name": "test"},
		}

		mock.Patch(c.OpLogPosition2GTID, func(p *primitive.Timestamp) string {
			return ""
		})
		mock.Patch(c.TransferDataAndColumnMapping, func(row map[string]interface{}) (columnMapping map[string]string) {
			return
		})
		data := c.BuildRowEvent(op)
		So(data, ShouldNotBeNil)
		So(data.SchemaName, ShouldEqual, "database")
		So(data.EventType, ShouldEqual, "update")
		So(data.Rows[1]["_id"], ShouldEqual, op.Id.(primitive.ObjectID).Hex())
		So(data.Rows[1]["name"], ShouldEqual, "test")
	})

	Convey("op.Operation u Data is nil", t, func() {
		c := new(MongoInput)
		op := &gtm.Op{
			Operation: "u",
			Id:        primitive.NewObjectID(),
			Namespace: "database.table",
			Data:      nil,
		}

		mock.Patch(c.OpLogPosition2GTID, func(p *primitive.Timestamp) string {
			return ""
		})
		mock.Patch(c.TransferDataAndColumnMapping, func(row map[string]interface{}) (columnMapping map[string]string) {
			return
		})
		data := c.BuildRowEvent(op)
		So(data, ShouldNotBeNil)
		So(data.SchemaName, ShouldEqual, "database")
		So(data.EventType, ShouldEqual, "update")
		So(data.Rows[1]["_id"], ShouldEqual, op.Id.(primitive.ObjectID).Hex())
		So(len(data.Rows[1]), ShouldEqual, 1)
	})

	Convey("op.Operation d", t, func() {
		c := new(MongoInput)
		op := &gtm.Op{
			Operation: "d",
			Id:        primitive.NewObjectID(),
			Namespace: "database.table",
			Data:      make(map[string]interface{}),
		}

		mock.Patch(c.OpLogPosition2GTID, func(p *primitive.Timestamp) string {
			return ""
		})
		mock.Patch(c.TransferDataAndColumnMapping, func(row map[string]interface{}) (columnMapping map[string]string) {
			return
		})
		data := c.BuildRowEvent(op)
		So(data, ShouldNotBeNil)
		So(data.SchemaName, ShouldEqual, "database")
		So(data.EventType, ShouldEqual, "delete")
		So(data.Rows[0]["_id"], ShouldEqual, op.Id.(primitive.ObjectID).Hex())

		// not data map
		op = &gtm.Op{
			Operation: "d",
			Id:        primitive.NewObjectID(),
			Namespace: "database.table",
		}
		data = c.BuildRowEvent(op)
		So(data, ShouldNotBeNil)
		So(data.SchemaName, ShouldEqual, "database")
		So(data.EventType, ShouldEqual, "delete")
		So(data.Rows[0]["_id"], ShouldEqual, op.Id.(primitive.ObjectID).Hex())
	})
}

func TestMongoInput_TransferDataAndColumnMapping(t *testing.T) {
	Convey("row is nil", t, func() {
		c := new(MongoInput)
		data := c.TransferDataAndColumnMapping(nil)
		So(data, ShouldBeNil)
	})

	Convey("row is map", t, func() {
		c := new(MongoInput)

		nowTime := time.Now()

		row := make(map[string]interface{}, 0)
		row["_id"] = primitive.NewObjectID().Hex()
		row["int8"] = int8(-8)
		row["uint8"] = uint8(8)
		row["int16"] = int16(-16)
		row["uint16"] = uint16(16)
		row["nil"] = nil
		row["time"] = nowTime
		row["int32"] = int32(-32)
		row["uint32"] = uint32(32)
		row["int64"] = int64(-64)
		row["uint64"] = uint64(64)
		row["float32"] = float32(9.99)
		row["float64"] = float64(88.88)
		row["bool"] = true
		row["string"] = "string"

		row["map"] = map[string]interface{}{"map_key1": 1, "map_key2": []string{"a", "b", "c"}}
		row["array"] = []string{"a", "b", "c"}

		list := make([]int, 0)
		list = append(list, 1)
		list = append(list, 2)
		row["slice"] = list

		type TypeStruct struct {
			Key  string
			Val  string
			Time time.Time
		}

		row["struct"] = TypeStruct{Key: "11", Val: "10000"}

		row["struct_pointer"] = &TypeStruct{Key: "22", Val: "20000"}

		data := c.TransferDataAndColumnMapping(row)

		So(len(data), ShouldEqual, len(row))
		for name, _ := range row {
			switch name {
			case "_id":
				So(data[name], ShouldEqual, "string")
				break
			case "map", "array", "slice":
				So(data[name], ShouldEqual, "Nullable(json)")
			case "string", "struct_pointer", "struct", "nil":
				So(data[name], ShouldEqual, "Nullable(string)")
			case "time":
				So(row[name], ShouldEqual, nowTime.Format("2006-01-02 15:04:05"))
				So(data[name], ShouldEqual, "Nullable(timestamp)")
			default:
				So(data[name], ShouldEqual, "Nullable("+name+")")
			}
		}
		So(row["nil"], ShouldBeNil)
		So(row["int8"], ShouldEqual, "-8")
		So(row["bool"], ShouldEqual, true)
	})
}

func TestMongoInput_BuildDropDatabaseQueryEvent(t *testing.T) {
	Convey("ddl drop database", t, func() {
		c := new(MongoInput)
		op := &gtm.Op{
			Operation: "c",
			Namespace: "database.table",
			Data:      make(map[string]interface{}),
		}
		data := c.BuildDropDatabaseQueryEvent(op)
		So(data.EventType, ShouldEqual, "sql")
		So(data.SchemaName, ShouldEqual, "database")
		So(data.Query, ShouldEqual, fmt.Sprintf("DROP DATABASE %s", "database"))
	})
}

func TestMongoInput_BuildDropTableQueryEvent(t *testing.T) {
	Convey("ddl drop table", t, func() {
		c := new(MongoInput)
		op := &gtm.Op{
			Operation: "c",
			Namespace: "database.table",
			Data:      make(map[string]interface{}),
		}
		data := c.BuildDropTableQueryEvent(op)
		So(data.EventType, ShouldEqual, "sql")
		So(data.SchemaName, ShouldEqual, "database")
		So(data.TableName, ShouldEqual, "table")
		So(data.Query, ShouldEqual, fmt.Sprintf("DROP TABLE %s", "table"))
	})
}

func TestMongoInput_BuildQueryEvent(t *testing.T) {
	Convey("sql build event", t, func() {
		c := new(MongoInput)
		op := &gtm.Op{
			Operation: "c",
			Namespace: "database.table",
			Data:      make(map[string]interface{}),
		}
		sql := "alert table"
		data := c.BuildQueryEvent(op, sql)
		So(data.EventType, ShouldEqual, "sql")
		So(data.SchemaName, ShouldEqual, "database")
		So(data.TableName, ShouldEqual, "table")
		So(data.Query, ShouldEqual, sql)
	})
}

func TestMongoInput_BuildCommitEvent(t *testing.T) {
	Convey("build commit event", t, func() {
		c := new(MongoInput)
		data := &outputDriver.PluginDataType{}
		commitEventData := c.BuildCommitEvent(data)
		So(commitEventData.EventType, ShouldEqual, "commit")
		So(commitEventData.SchemaName, ShouldEqual, data.SchemaName)
	})
}
