package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Schema struct {
	field string
	ftype string
}

var tableSchema map[string][]*Schema // 存储表的所有字段和类型，用于查看表是否船舰以及表字段是否有变化
var tRwMutex sync.RWMutex

// 特殊时间格式，clickhouse不识别，需要转空格格式
var re = regexp.MustCompile("^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]")

func IsTableInSchema(tb string) bool {
	tRwMutex.RLock()
	defer tRwMutex.RUnlock()
	if tableSchema != nil {
		_, ok := tableSchema[tb]
		return ok
	}
	return false
}

func GetTableSchema(table string) []*Schema {
	tRwMutex.RLock()
	defer tRwMutex.RUnlock()
	if tableSchema != nil {
		if schema, ok := tableSchema[table]; ok {
			return schema
		}
	}
	return make([]*Schema, 0)
}

func GetTableFields(table string) []string {
	tRwMutex.RLock()
	defer tRwMutex.RUnlock()
	if tableSchema != nil {
		if schema, ok := tableSchema[table]; ok {
			if schema != nil && len(schema) > 0 {
				fields := make([]string, 0)
				for _, fs := range schema {
					fields = append(fields, fs.field)
				}
				return fields
			}
		}
	}
	return make([]string, 0)
}

func SyncBatch2Ck(conn driver.Conn, objects []*ChangeStream) error {
	if objects == nil || len(objects) <= 0 {
		return nil
	}

	db, tb := objects[0].Ns.Database, objects[0].Ns.Collection

	if err := CheckAndCreateTable(conn, db, tb, objects[0]); err != nil {
		return err
	}

	// 如果表字段变化，修改表字段
	if err := CheckAndAlterTableField(conn, db, tb, objects[0], false); err != nil {
		return err
	}
	// todo: 如果索引变化，数据存储可能有较大改动，暂不处理，后面测试一下更改索引是实时还是离线操作
	if err := CheckAndAlterTablePrimary(conn, db, tb, objects[0]); err != nil {
		return err
	}
	// 构造sql语句，这里拼接的字符串非常大，不能直接+拼接
	var query strings.Builder
	// 构造字段名元组，顺序与schema一致
	fields := GetTableFields(db + "." + tb)
	fieldTupleString := GetTableFieldsTupleString(fields)
	query.WriteString("INSERT INTO " + db + "." + tb + " " + fieldTupleString + " VALUES ")
	// 批量构建数据元组，注意字段和value要保持顺序一致，因此需要将schema传入构建value的函数
	schema := GetTableSchema(db + "." + tb)
	for idx, obj := range objects {
		if idx > 0 {
			query.WriteString(", ")
		}
		query.WriteString(GetTableValuesTupleString(schema, obj))
	}
	query.WriteString(";")
	// 查询
	if err := conn.Exec(context.Background(), query.String()); err != nil {
		return err
	}
	return nil
}

func QueryAndUpdateTableSchema(conn driver.Conn, db, tb string) ([]*Schema, error) {
	schema := make([]*Schema, 0)
	rows, err := conn.Query(context.Background(),
		"SELECT DISTINCT name, type FROM system.columns WHERE database='"+db+"' AND table='"+tb+"';")
	if err != nil {
		return schema, err
	}
	for rows.Next() {
		var field, ftype string
		if err = rows.Scan(&field, &ftype); err != nil {
			return schema, err
		}
		schema = append(schema, &Schema{field, ftype})
	}
	// 查询schema存在，更新缓存的schema map
	if len(schema) > 0 {
		tRwMutex.Lock()
		if tableSchema == nil {
			tableSchema = make(map[string][]*Schema)
		}
		tableSchema[db+"."+tb] = schema
		tRwMutex.Unlock()
	}
	return schema, nil
}

func CheckAndCreateTable(conn driver.Conn, db, tb string, obj *ChangeStream) error {
	// 查询缓存是否存在表schema
	if IsTableInSchema(db + "." + tb) {
		return nil
	}
	// 没有缓存，ClickHouse查询表结构并更新缓存
	schema, err := QueryAndUpdateTableSchema(conn, db, tb)
	if err != nil {
		return err
	} else if len(schema) > 0 {
		return nil
	}
	// ClickHouse表不存在，开始建表
	fields := GetObjectFields(obj) // 从obj key获取需要建表字段
	if len(fields) <= 0 {
		return errors.New("clickhouse create table failed")
	}
	// 如果库不存在则建库
	if err := conn.Exec(context.Background(), "CREATE DATABASE IF NOT EXISTS "+db+";"); err != nil {
		return err
	}
	// 构建建表语句
	schemaTupleString := ""
	for _, field := range fields {
		ftype := GetTableValueTypeString(field, obj)
		if len(schemaTupleString) > 0 {
			schemaTupleString += ", "
		}
		schemaTupleString += fmt.Sprintf("`%s` %s", field, ftype)
		schema = append(schema, &Schema{field, ftype})
	}
	// 使用ReplacingMergeTree确保重复发送数据会被去重，选择最后更新的
	engine := CreateEngine(obj)
	// 创建主键和排序键，排序键用于存储顺序和去重，主键是内存索引，目前保持一致
	primary := CreatePrimary(obj)
	order := primary
	query := "CREATE TABLE " + db + "." + tb + "\n" +
		"(" + schemaTupleString + ")\n" +
		"ENGINE = " + engine + "\n" +
		"PRIMARY KEY " + GetTableFieldsTupleString(primary) + "\n" +
		"ORDER BY " + GetTableFieldsTupleString(order) + "\n" +
		CreatePartition(obj)
	// 发送建表请求
	if err := conn.Exec(context.Background(), query); err != nil {
		return err
	}
	// 新建表字段信息加到schema map
	tRwMutex.Lock()
	if tableSchema == nil {
		tableSchema = make(map[string][]*Schema)
	}
	tableSchema[db+"."+tb] = schema
	tRwMutex.Unlock()
	return nil
}

// 从object keys筛选不在schema fields的新增字段
func GetTableFieldChange(db, table string, obj *ChangeStream) []string {
	schemaFieldMap := make(map[string]bool)
	for _, schema := range GetTableSchema(db + "." + table) {
		schemaFieldMap[schema.field] = true
	}
	addFields := make([]string, 0)
	for _, field := range GetObjectFields(obj) {
		if _, ok := schemaFieldMap[field]; !ok {
			addFields = append(addFields, field)
		}
	}
	sort.Strings(addFields)
	return addFields
}

// 修改表字段（目前只删不减，根据object新字段补建表字段，object有可能缺字段，减字段可能误删）
func CheckAndAlterTableField(conn driver.Conn, db, table string, obj *ChangeStream, check bool) error {
	// 根据参数决定是否实际查ClickHouse表结构
	if check {
		if _, err := QueryAndUpdateTableSchema(conn, db, table); err != nil {
			return err
		}
	}
	addFields := GetTableFieldChange(db, table, obj)
	if addFields != nil && len(addFields) > 0 {
		addSchema := make([]*Schema, 0)
		query := "ALTER TABLE " + db + "." + table + "\n"
		for idx, field := range addFields {
			ftype := GetTableValueTypeString(field, obj)
			addSchema = append(addSchema, &Schema{field, ftype})
			query += fmt.Sprintf("ADD COLUMN `%s` %s", field, ftype)
			if idx+1 < len(addFields) {
				query += ","
			} else {
				query += ";"
			}
		}
		if err := conn.Exec(context.Background(), query); err != nil {
			return err
		}
		// 更改表字段成功，新字段信息加到schema map
		tRwMutex.Lock()
		if tableSchema == nil {
			tableSchema = make(map[string][]*Schema)
		}
		for _, schema := range addSchema {
			tableSchema[db+"."+table] = append(tableSchema[db+"."+table], schema)
		}
		tRwMutex.Unlock()
	}
	return nil
}

// 引擎策略: ReplacingMergeTree去重，version选择update_at最晚的
func CreateEngine(obj *ChangeStream) string {
	if val, ok := obj.FullDocument["updated_at"]; ok {
		valueType := fmt.Sprintf("%v", reflect.TypeOf(val))
		if valueType == "primitive.DateTime" {
			return "ReplacingMergeTree(`updated_at`)"
		}
	}
	return "ReplacingMergeTree()"
}

// 分区策略: 如果配置不自动分库，且存在app_id则用于分区，否则用日期分区
func CreatePartition(obj *ChangeStream) string {
	if _, ok := obj.FullDocument["created_at"]; ok &&
		strings.HasPrefix(GetTableValueTypeString("created_at", obj), "DateTime") {
		return "PARTITION BY toYYYYMM(`created_at`)\n"
	}
	if _, ok := obj.FullDocument["date"]; ok {
		return "PARTITION BY toInt64(`date`/100)\n"
	}
	return ""
}

// 创建主键（内存索引，且为排序键前缀），暂时采用通用策略
func CreatePrimary(obj *ChangeStream) []string {
	primary := make([]string, 0)
	if obj == nil || obj.FullDocument == nil {
		return primary
	}
	// 时间字段在最高优先级，大量查询需要根据时间筛选并排序，且时间跟userId有一定的正相关，查userId也能大量过滤
	if _, ok := obj.FullDocument["created_at"]; ok {
		primary = append(primary, "created_at")
	}
	// 报表查询需要用hour和date，放在created_at后面
	if _, ok := obj.FullDocument["hour"]; ok {
		primary = append(primary, "hour")
	}
	if _, ok := obj.FullDocument["date"]; ok {
		primary = append(primary, "date")
	}
	// _id，放入主键和排序键确保ReplaceMergeTree去重
	if _, ok := obj.FullDocument["_id"]; ok {
		primary = append(primary, "_id")
	}
	if _, ok := obj.FullDocument["id"]; ok {
		primary = append(primary, "id")
	}
	// user_id查询较多，基数较大也跟时间有一定正相关性(生命周期不会太长)，放入主键可以快速锁定范围
	if _, ok := obj.FullDocument["user_id"]; ok {
		primary = append(primary, "user_id")
	}
	return primary
}

// todo: 自动更改表主键内存索引或者二级跳数索引
func CheckAndAlterTablePrimary(conn driver.Conn, db, table string, obj *ChangeStream) error {
	return nil
}

func GetObjectFields(obj *ChangeStream) []string {
	fields := make([]string, 0)
	if obj != nil && obj.FullDocument != nil {
		for key := range obj.FullDocument {
			if strings.Contains(key, "`") {
				key = strings.ReplaceAll(key, "`", "")
			}
			fields = append(fields, key)
		}
	}
	sort.Strings(fields)
	return fields
}

func GetTableFieldsTupleString(fields []string) string {
	var ret strings.Builder
	if fields != nil && len(fields) > 0 {
		ret.WriteString("(")
		for _, field := range fields {
			if ret.Len() > 1 {
				ret.WriteString(", ")
			}
			ret.WriteString("`" + field + "`")
		}
		ret.WriteString(")")
	}
	return ret.String()
}

// 将数据原始类型转为ClickHouse字段类型，目前只分为数字、String、时间三大类型，其他非基本类型转为String
func GetTableValueTypeString(field string, obj *ChangeStream) string {
	if obj != nil && obj.FullDocument != nil {
		if val, ok := obj.FullDocument[field]; ok {
			valueType := fmt.Sprintf("%v", reflect.TypeOf(val))
			if strings.HasPrefix(valueType, "int") {
				return "I" + valueType[1:]
			}
			if strings.HasPrefix(valueType, "uint") {
				return "UI" + valueType[2:]
			}
			if strings.HasPrefix(valueType, "float") { // 有时候int型会被识别为float
				if strings.HasSuffix(field, "_id") || strings.HasSuffix(field, "_no") {
					return strings.ReplaceAll(valueType, "float", "Int")
				}
				return "F" + valueType[1:]
			}
			if strings.Contains(valueType, "Date") {
				return "DateTime64"
			}
		}
	}
	return "String"
}

func GetTableValueQueryString(field, ftype string, obj *ChangeStream) string {
	if obj != nil && obj.FullDocument != nil {
		if val, ok := obj.FullDocument[field]; ok {
			if val != nil { // 如果存在nil值最后处理，正常不会有，mongo返回数据做了默认值处理
				// 根据数据真实类型处理，返回需要跟建表时映射类型一致，注意String类型查询时需要加引号
				valueType := fmt.Sprintf("%v", reflect.TypeOf(val))
				if valueType == "primitive.ObjectID" { // ObjectId类型需要调用特殊方法转字符串
					return "'" + val.(primitive.ObjectID).Hex() + "'"
				} else if valueType == "primitive.DateTime" { // 时间类型，这里丢失了毫秒
					if strings.Contains(ftype, "Int") {
						return strconv.FormatInt(val.(primitive.DateTime).Time().Unix(), 10)
					}
					return strconv.FormatInt(val.(primitive.DateTime).Time().Unix()*1000, 10)
				} else if strings.HasPrefix(ftype, "DateTime") {
					// 毫秒类型时间戳可以直接传入到clickhouse自动转为时间，其他的时间转带引号的字符串类型
					if strings.HasPrefix(valueType, "int") ||
						strings.HasPrefix(valueType, "uint") ||
						strings.HasPrefix(valueType, "float") {
						val = AutoToInt(val)
						if val.(int) > 1600000000 && val.(int) < 2000000000 {
							val = val.(int) * 1000
						}
						return AutoToString(val)
					}
					// 目标类型DateTime但是数据是clickhouse无法自动识别的带T时间格式，需要转换
					switch val.(type) {
					case string:
						s := val.(string)
						if re.MatchString(s) {
							s = strings.ReplaceAll(s[:19], "T", " ")
						}
						return "'" + s + "'"
					}
				} else if strings.Contains(ftype, "Int") {
					// 目标类型int，可能是时间，尝试转换时间
					switch val.(type) {
					case string:
						s := val.(string)
						if re.MatchString(s) {
							s = strings.ReplaceAll(s[:19], "T", " ")
						}
						// 兼容带T和空格两种时间格式
						tm, err := time.Parse("2006-01-02 15:04:05", s)
						if err == nil {
							return strconv.FormatInt(tm.Unix(), 10)
						}
					}
					return AutoToString(AutoToInt(val))
				} else if strings.HasPrefix(valueType, "float") { // 有时候int型会被识别为float
					if strings.Contains(ftype, "Int") {
						val = AutoToInt(val)
					}
					return AutoToString(val)
				} else if strings.HasPrefix(ftype, "Float") {
					if !strings.HasPrefix(valueType, "float") { // 其他类型转float，先工具类转int再转，否则会失败
						val = AutoToInt(val)
					}
					return AutoToString(val)
				} else { // 其他类型: 数字或复杂类型，通用方法转字符串，除数字外都当作String类型加上引号
					s := AutoToString(val)
					if strings.HasPrefix(valueType, "int") ||
						strings.HasPrefix(valueType, "uint") ||
						strings.HasPrefix(valueType, "float") { // 数字类型，不加引号
						return s
					} else { // 其他类型当字符串处理，查询时加引号，特殊字符转义，注意\转义在最前面，否则会重复转
						s = strings.ReplaceAll(s, "\\", "\\\\")
						s = strings.ReplaceAll(s, "'", "\\'")
						return "'" + s + "'"
					}
				}
			}
		}
	}
	// 数据缺少这个字段（mongo不要求所有字段齐全），根据表结构类型设置默认值写入
	if strings.Contains(ftype, "Int") || strings.HasPrefix(ftype, "Float") {
		return "0"
	} else if strings.HasPrefix(ftype, "DateTime") {
		// 写入整数0，字符串'0000-00-00 00:00:00'，实际写入都是'1970-01-01 00:00:00'
		return "'0000-00-00 00:00:00'"
	}
	// 空字符串
	return "''"
}

func GetTableValuesTupleString(schema []*Schema, obj *ChangeStream) string {
	var ret strings.Builder
	if schema != nil && len(schema) > 0 {
		ret.WriteString("(")
		for _, fs := range schema {
			if ret.Len() > 1 {
				ret.WriteString(", ")
			}
			ret.WriteString(GetTableValueQueryString(fs.field, fs.ftype, obj))
		}
		ret.WriteString(")")
	}
	return ret.String()
}
