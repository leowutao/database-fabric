package test

//import (
//	"fmt"
//	"github.com/bidpoc/database-fabric-cc/db"
//)
//
//const (
//	KEY = "dae735e6-6a5b-4fc8-bf4e-b4a67a671b37"
//	VALUE = "JTdCJTIyYXJ0aXN0JTIyJTNBJTIwJTIyJTIyJTJDJTIwJTIyY291bnRyeSUyMiUzQSUyMCUyMkNOJTIyJTJDJTIwJTIyY292ZXJzJTIyJTNBJTIwJTIyJTIyJTJDJTIwJTIyY3JlYXRpb25fZGF0ZSUyMiUzQSUyMCUyMiUyMiUyQyUyMCUyMmRlc2NyaXB0aW9uJTIyJTNBJTIwJTIyJTNDcCUzRSU1Q3U2M2NmJTVDdThmZjAlM0MvcCUzRSUyMiUyQyUyMCUyMmRpZ2l0YWxfZmluZ2VycHJpbnQlMjIlM0ElMjAlMjIlMjIlMkMlMjAlMjJkaW1lbnNpb25zJTIyJTNBJTIwJTIyJTIyJTJDJTIwJTIyZG9jcyUyMiUzQSUyMCUyMiUyMiUyQyUyMCUyMmV4dHJhX2ZpZWxkcyUyMiUzQSUyMCU1QiU3QiUyMmZpZWxkJTIyJTNBJTIwJTdCJTIyZGVmYXVsdF92YWx1ZSUyMiUzQSUyMCUyMmY0JTIyJTJDJTIwJTIyZGVzY3JpcHRpb24lMjIlM0ElMjAlMjIlMjIlMkMlMjAlMjJpZCUyMiUzQSUyMDExJTJDJTIwJTIyaXNfcHVibGljJTIyJTNBJTIwZmFsc2UlMkMlMjAlMjJpc19yZXF1aXJlZCUyMiUzQSUyMGZhbHNlJTJDJTIwJTIybmFtZSUyMiUzQSUyMCUyMmY0JTIyJTJDJTIwJTIydHlwZSUyMiUzQSUyMCUyMm51bWJlciUyMiU3RCUyQyUyMCUyMmlkJTIyJTNBJTIwNTAlMkMlMjAlMjJ2YWx1ZSUyMiUzQSUyMCUyMnYxMSUyMiU3RCUyQyUyMCU3QiUyMmZpZWxkJTIyJTNBJTIwJTdCJTIyZGVmYXVsdF92YWx1ZSUyMiUzQSUyMCUyMnYzJTIyJTJDJTIwJTIyZGVzY3JpcHRpb24lMjIlM0ElMjAlMjIlMjIlMkMlMjAlMjJpZCUyMiUzQSUyMDEwJTJDJTIwJTIyaXNfcHVibGljJTIyJTNBJTIwdHJ1ZSUyQyUyMCUyMmlzX3JlcXVpcmVkJTIyJTNBJTIwZmFsc2UlMkMlMjAlMjJuYW1lJTIyJTNBJTIwJTIyJTVDdTRmMzAlNUN1NTAzYyUyMHYzJTIyJTJDJTIwJTIydHlwZSUyMiUzQSUyMCUyMnRleHQlMjIlN0QlMkMlMjAlMjJpZCUyMiUzQSUyMDUxJTJDJTIwJTIydmFsdWUlMjIlM0ElMjAlMjJ2MTAlMjIlN0QlNUQlMkMlMjAlMjJpc19wdWJsaWMlMjIlM0ElMjB0cnVlJTJDJTIwJTIybG9jYXRpb24lMjIlM0ElMjAlMjIlMjIlMkMlMjAlMjJtZWRpdW0lMjIlM0ElMjAlMjIlM0NwJTNFJTVDdThmZDklNUN1OTFjYyU1Q3U2NjJmJTVDdTY3NTAlNUN1OGQyOCUzQy9wJTNFJTIyJTJDJTIwJTIybmZjX2NvZGUlMjIlM0ElMjAlMjIlMjIlMkMlMjAlMjJudW1iZXIlMjIlM0ElMjAlMjIxLjE1NDUwMzYxNjQlMjIlMkMlMjAlMjJzdG9yZV9jb2RlJTIyJTNBJTIwJTIyJTIyJTJDJTIwJTIydGl0bGUlMjIlM0ElMjAlMjJzdHJpbmclMjAxMTAwMSUyMiUyQyUyMCUyMnVzZXJfdXVpZCUyMiUzQSUyMCUyMjYwN2MzZDg0LTZiOTQtNDM4MC1iYWUyLTY3M2M3NzVmMjVhMiUyMiUyQyUyMCUyMnV1aWQlMjIlM0ElMjAlMjJkYWU3MzVlNi02YTViLTRmYzgtYmY0ZS1iNGE2N2E2NzFiMzclMjIlMkMlMjAlMjJ2YWx1YXRpb24lMjIlM0ElMjAlMjIlNUN1NGUwMCU1Q3U0ZWJmJTIyJTdE"
//
//	//{"artist": "", "country": "CN", "covers": "", "creation_date": "", "description": "<p>\u63cf\u8ff0</p>", "digital_fingerprint": "", "dimensions": "",
//	// "docs": "", "extra_fields": [{"field": {"default_value": "f4", "description": "", "id": 11, "is_public": false,
//	// "is_required": false, "name": "f4", "type": "number"}, "id": 50, "value": "v11"}, {"field": {"default_value": "v3",
//	// "description": "", "id": 10, "is_public": true, "is_required": false, "name": "\u4f30\u503c v3", "type": "text"}, "id": 51,
//	// "value": "v10"}], "is_public": true, "location": "", "medium": "<p>\u8fd9\u91cc\u662f\u6750\u8d28</p>", "nfc_code": "",
//	// "number": "1.1545036164", "store_code": "", "title": "string 11001", "user_uuid": "607c3d84-6b94-4380-bae2-673c775f25a2",
//	// "uuid": "dae735e6-6a5b-4fc8-bf4e-b4a67a671b37", "valuation": "\u4e00\u4ebf"}
//)
//
//var COLLECTION_TABLE = db.Table{"collection",[]db.Column{
//	{"id", db.INT,"",false,"test"},
//	{"artist", db.VARCHAR,22.2222,true,"test"},
//	{"country", db.VARCHAR,nil,false,"test"},
//	{"covers", db.DECIMAL,1,false,"test"},
//	{"creation_date", db.INT,"221",false,"test"},
//	{"description", db.VARCHAR,"",false,"test"},
//	{"digital_fingerprint", db.VARCHAR,"",false,"test"},
//	{"dimensions", db.VARCHAR,"",false,"test"},
//	{"docs", db.VARCHAR,"",false,"test"},
//	{"is_public", db.BOOL,"",false,"test"},
//	{"location", db.VARCHAR,"",false,"test"},
//	{"medium", db.VARCHAR,"",false,"test"},
//	{"nfc_code", db.VARCHAR,"二进制",true,"test"},
//	{"number", db.VARCHAR,"",false,"test"},
//	{"store_code", db.VARCHAR,"",false,"test"},
//	{"title", db.VARCHAR,"",false,"test"},
//	{"user_uuid", db.VARCHAR,"",false,"test"},
//	{"uuid", db.VARCHAR,"",false,"test"},
//	{"valuation", db.DECIMAL,"424555545455165165.55489881455554488",false,"test"},
//	{"category_id", db.INT,"1",false,"test"},
//}, db.PrimaryKey{"id", true},
//[]db.ForeignKey{{"category_id", db.ReferenceKey{"category","id"}}}}
//
//var CATEGORY_TABLE = db.Table{"category",[]db.Column{
//	{"id", db.INT,"",false,"test"},
//	{"name", db.VARCHAR,"",false,"test"},
//	{"create_at", db.VARCHAR,"",false,"test"},
//	{"update_at", db.VARCHAR,"",false,"test"},
//}, db.PrimaryKey{"id", true},[]db.ForeignKey{}}
//
//var TEMPLATE_TABLE = db.Table{"template",[]db.Column{
//	{"id", db.INT,"",false,"test"},
//	{"name", db.VARCHAR,"",false,"test"},
//	{"status", db.VARCHAR,"",false,"test"},
//	{"create_at", db.VARCHAR,"",false,"test"},
//	{"update_at", db.VARCHAR,"",false,"test"},
//	{"category_id", db.INT,"1",false,"test"},
//}, db.PrimaryKey{"id", true},
//[]db.ForeignKey{{"category_id", db.ReferenceKey{"category", "id"}}}}
//
//var TEMPLATE_DETAIL_TABLE = db.Table{"template_detail",[]db.Column{
//	{"id", db.INT,nil,true,"test"},
//	{"name", db.VARCHAR,"",false,"test"},
//	{"status", db.VARCHAR,"",false,"test"},
//	{"create_at", db.VARCHAR,"",false,"test"},
//	{"update_at", db.VARCHAR,"",false,"test"},
//	{"template_id", db.INT,"1",false,"test"},
//}, db.PrimaryKey{"id", true},
//[]db.ForeignKey{{"template_id", db.ReferenceKey{"template", "id"}}}}
//
//
//var COLLECTION_FIELD_TABLE = db.Table{"collection_field",[]db.Column{
//	{"id", db.INT,"",true,"test"},
//	{"name", db.VARCHAR,"",false,"test"},
//	{"value", db.VARCHAR,"",false,"test"},
//	{"collection_id", db.INT,"1",false,"test"},
//	{"template_detail_id", db.INT,"1",false,"test"},
//}, db.PrimaryKey{"id", true},
//[]db.ForeignKey{{"collection_id", db.ReferenceKey{"collection", "id"}},{"template_detail_id", db.ReferenceKey{"template_detail", "id"}}}}
//
//
//var TABLES = []db.Table{
//	CATEGORY_TABLE,
//	TEMPLATE_TABLE,
//	TEMPLATE_DETAIL_TABLE,
//	COLLECTION_TABLE,
//	COLLECTION_FIELD_TABLE,
//}
//
//var COLLECTION_SCHEMA = db.Schema{"collection",2, db.Model{
//	"collection",COLLECTION_TABLE.Name,false,[]db.Model{
//		{"collectionFields",COLLECTION_FIELD_TABLE.Name,true,[]db.Model{}},
//	},
//}}
//
//var SCHEMAS = []db.Schema{
//	COLLECTION_SCHEMA,
//}
//
//var BASE64_TABLES = []string{
//	"eyJuYW1lIjoiY2F0ZWdvcnkiLCJjb2x1bW5zIjpbeyJuYW1lIjoiaWQiLCJ0eXBlIjoxLCJkZWZhdWx0IjoiIiwibm90TnVsbCI6ZmFsc2UsImRlc2MiOiJ0ZXN0In0seyJuYW1lIjoibmFtZSIsInR5cGUiOjMsImRlZmF1bHQiOiIiLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifV0sInByaW1hcnlLZXkiOnsiY29sdW1uIjoiaWQiLCJhdXRvSW5jcmVtZW50Ijp0cnVlfSwiZm9yZWlnbktleXMiOltdfQo=",
//	"eyJuYW1lIjoidGVtcGxhdGUiLCJjb2x1bW5zIjpbeyJuYW1lIjoiaWQiLCJ0eXBlIjoxLCJkZWZhdWx0IjoiIiwibm90TnVsbCI6ZmFsc2UsImRlc2MiOiJ0ZXN0In0seyJuYW1lIjoibmFtZSIsInR5cGUiOjMsImRlZmF1bHQiOiIiLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifSx7Im5hbWUiOiJzdGF0dXMiLCJ0eXBlIjozLCJkZWZhdWx0IjoiIiwibm90TnVsbCI6ZmFsc2UsImRlc2MiOiJ0ZXN0In0seyJuYW1lIjoiY3JlYXRlX2F0IiwidHlwZSI6MywiZGVmYXVsdCI6IiIsIm5vdE51bGwiOmZhbHNlLCJkZXNjIjoidGVzdCJ9LHsibmFtZSI6InVwZGF0ZV9hdCIsInR5cGUiOjMsImRlZmF1bHQiOiIiLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifSx7Im5hbWUiOiJjYXRlZ29yeV9pZCIsInR5cGUiOjEsImRlZmF1bHQiOiIxIiwibm90TnVsbCI6ZmFsc2UsImRlc2MiOiJ0ZXN0In1dLCJwcmltYXJ5S2V5Ijp7ImNvbHVtbiI6ImlkIiwiYXV0b0luY3JlbWVudCI6dHJ1ZX0sImZvcmVpZ25LZXlzIjpbeyJjb2x1bW4iOiJjYXRlZ29yeV9pZCIsInJlZmVyZW5jZSI6eyJ0YWJsZSI6ImNhdGVnb3J5IiwiY29sdW1uIjoiaWQifX1dfQo=",
//	"eyJuYW1lIjoidGVtcGxhdGVfZGV0YWlsIiwiY29sdW1ucyI6W3sibmFtZSI6ImlkIiwidHlwZSI6MSwiZGVmYXVsdCI6bnVsbCwibm90TnVsbCI6dHJ1ZSwiZGVzYyI6InRlc3QifSx7Im5hbWUiOiJuYW1lIiwidHlwZSI6MywiZGVmYXVsdCI6IiIsIm5vdE51bGwiOmZhbHNlLCJkZXNjIjoidGVzdCJ9LHsibmFtZSI6InN0YXR1cyIsInR5cGUiOjMsImRlZmF1bHQiOiIiLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifSx7Im5hbWUiOiJjcmVhdGVfYXQiLCJ0eXBlIjozLCJkZWZhdWx0IjoiIiwibm90TnVsbCI6ZmFsc2UsImRlc2MiOiJ0ZXN0In0seyJuYW1lIjoidXBkYXRlX2F0IiwidHlwZSI6MywiZGVmYXVsdCI6IiIsIm5vdE51bGwiOmZhbHNlLCJkZXNjIjoidGVzdCJ9LHsibmFtZSI6InRlbXBsYXRlX2lkIiwidHlwZSI6MSwiZGVmYXVsdCI6IjEiLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifV0sInByaW1hcnlLZXkiOnsiY29sdW1uIjoiaWQiLCJhdXRvSW5jcmVtZW50Ijp0cnVlfSwiZm9yZWlnbktleXMiOlt7ImNvbHVtbiI6InRlbXBsYXRlX2lkIiwicmVmZXJlbmNlIjp7InRhYmxlIjoidGVtcGxhdGUiLCJjb2x1bW4iOiJpZCJ9fV19Cg==",
//	"eyJuYW1lIjoiY29sbGVjdGlvbiIsImNvbHVtbnMiOlt7Im5hbWUiOiJpZCIsInR5cGUiOjEsImRlZmF1bHQiOiIiLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifSx7Im5hbWUiOiJhcnRpc3QiLCJ0eXBlIjozLCJkZWZhdWx0IjoyMi4yMjIyLCJub3ROdWxsIjp0cnVlLCJkZXNjIjoidGVzdCJ9LHsibmFtZSI6ImNvdW50cnkiLCJ0eXBlIjozLCJkZWZhdWx0IjpudWxsLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifSx7Im5hbWUiOiJjb3ZlcnMiLCJ0eXBlIjoyLCJkZWZhdWx0IjoxLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifSx7Im5hbWUiOiJjcmVhdGlvbl9kYXRlIiwidHlwZSI6MSwiZGVmYXVsdCI6IjIyMSIsIm5vdE51bGwiOmZhbHNlLCJkZXNjIjoidGVzdCJ9LHsibmFtZSI6ImRlc2NyaXB0aW9uIiwidHlwZSI6MywiZGVmYXVsdCI6IiIsIm5vdE51bGwiOmZhbHNlLCJkZXNjIjoidGVzdCJ9LHsibmFtZSI6ImRpZ2l0YWxfZmluZ2VycHJpbnQiLCJ0eXBlIjozLCJkZWZhdWx0IjoiIiwibm90TnVsbCI6ZmFsc2UsImRlc2MiOiJ0ZXN0In0seyJuYW1lIjoiZGltZW5zaW9ucyIsInR5cGUiOjMsImRlZmF1bHQiOiIiLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifSx7Im5hbWUiOiJkb2NzIiwidHlwZSI6MywiZGVmYXVsdCI6IiIsIm5vdE51bGwiOmZhbHNlLCJkZXNjIjoidGVzdCJ9LHsibmFtZSI6ImlzX3B1YmxpYyIsInR5cGUiOjQsImRlZmF1bHQiOiIiLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifSx7Im5hbWUiOiJsb2NhdGlvbiIsInR5cGUiOjMsImRlZmF1bHQiOiIiLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifSx7Im5hbWUiOiJtZWRpdW0iLCJ0eXBlIjozLCJkZWZhdWx0IjoiIiwibm90TnVsbCI6ZmFsc2UsImRlc2MiOiJ0ZXN0In0seyJuYW1lIjoibmZjX2NvZGUiLCJ0eXBlIjozLCJkZWZhdWx0Ijoi5LqM6L+b5Yi2Iiwibm90TnVsbCI6dHJ1ZSwiZGVzYyI6InRlc3QifSx7Im5hbWUiOiJudW1iZXIiLCJ0eXBlIjozLCJkZWZhdWx0IjoiIiwibm90TnVsbCI6ZmFsc2UsImRlc2MiOiJ0ZXN0In0seyJuYW1lIjoic3RvcmVfY29kZSIsInR5cGUiOjMsImRlZmF1bHQiOiIiLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifSx7Im5hbWUiOiJ0aXRsZSIsInR5cGUiOjMsImRlZmF1bHQiOiIiLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifSx7Im5hbWUiOiJ1c2VyX3V1aWQiLCJ0eXBlIjozLCJkZWZhdWx0IjoiIiwibm90TnVsbCI6ZmFsc2UsImRlc2MiOiJ0ZXN0In0seyJuYW1lIjoidXVpZCIsInR5cGUiOjMsImRlZmF1bHQiOiIiLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifSx7Im5hbWUiOiJ2YWx1YXRpb24iLCJ0eXBlIjoyLCJkZWZhdWx0IjoiNDI0NTU1NTQ1NDU1MTY1MTY1LjU1NDg5ODgxNDU1NTU0NDg4Iiwibm90TnVsbCI6ZmFsc2UsImRlc2MiOiJ0ZXN0In0seyJuYW1lIjoiY2F0ZWdvcnlfaWQiLCJ0eXBlIjoxLCJkZWZhdWx0IjoiMSIsIm5vdE51bGwiOmZhbHNlLCJkZXNjIjoidGVzdCJ9XSwicHJpbWFyeUtleSI6eyJjb2x1bW4iOiJpZCIsImF1dG9JbmNyZW1lbnQiOnRydWV9LCJmb3JlaWduS2V5cyI6W3siY29sdW1uIjoiY2F0ZWdvcnlfaWQiLCJyZWZlcmVuY2UiOnsidGFibGUiOiJjYXRlZ29yeSIsImNvbHVtbiI6ImlkIn19XX0K",
//	"eyJuYW1lIjoiY29sbGVjdGlvbl9maWVsZCIsImNvbHVtbnMiOlt7Im5hbWUiOiJpZCIsInR5cGUiOjEsImRlZmF1bHQiOiIiLCJub3ROdWxsIjp0cnVlLCJkZXNjIjoidGVzdCJ9LHsibmFtZSI6Im5hbWUiLCJ0eXBlIjozLCJkZWZhdWx0IjoiIiwibm90TnVsbCI6ZmFsc2UsImRlc2MiOiJ0ZXN0In0seyJuYW1lIjoidmFsdWUiLCJ0eXBlIjozLCJkZWZhdWx0IjoiIiwibm90TnVsbCI6ZmFsc2UsImRlc2MiOiJ0ZXN0In0seyJuYW1lIjoiY29sbGVjdGlvbl9pZCIsInR5cGUiOjEsImRlZmF1bHQiOiIxIiwibm90TnVsbCI6ZmFsc2UsImRlc2MiOiJ0ZXN0In0seyJuYW1lIjoidGVtcGxhdGVfZGV0YWlsX2lkIiwidHlwZSI6MSwiZGVmYXVsdCI6IjEiLCJub3ROdWxsIjpmYWxzZSwiZGVzYyI6InRlc3QifV0sInByaW1hcnlLZXkiOnsiY29sdW1uIjoiaWQiLCJhdXRvSW5jcmVtZW50Ijp0cnVlfSwiZm9yZWlnbktleXMiOlt7ImNvbHVtbiI6ImNvbGxlY3Rpb25faWQiLCJyZWZlcmVuY2UiOnsidGFibGUiOiJjb2xsZWN0aW9uIiwiY29sdW1uIjoiaWQifX0seyJjb2x1bW4iOiJ0ZW1wbGF0ZV9kZXRhaWxfaWQiLCJyZWZlcmVuY2UiOnsidGFibGUiOiJ0ZW1wbGF0ZV9kZXRhaWwiLCJjb2x1bW4iOiJpZCJ9fV19Cg==",
//}
//
//var BASE64_SCHEMAS = []string{
//	"eyJuYW1lIjoiY29sbGVjdGlvbiIsImxheWVyTnVtIjoyLCJtb2RlbCI6eyJuYW1lIjoiY29sbGVjdGlvbiIsInRhYmxlIjoiY29sbGVjdGlvbiIsImFycmF5IjpmYWxzZSwibW9kZWxzIjpbeyJuYW1lIjoiYWRkQ29sbGVjdGlvbkZpZWxkcyIsInRhYmxlIjoiY29sbGVjdGlvbl9maWVsZCIsImFycmF5Ijp0cnVlLCJtb2RlbHMiOltdfV19fSA=",
//}
//
//var BASE64_TABLE_ROWS = map[string]string{
//
//}
//
//var BASE64_SCHEMA_ROWS = map[string]string{
//
//}
//
//var TXID_NUM = 0000
//
////func TestChaincode() {
////	var t = new(db.DbManager)
////	var stub= new(TestChaincodeStub)
////	collectionKey := "collection_org1"
////	stub.Args = []string{collectionKey}
////
////	///////////////// table //////////////////
////	op := "table"
////	fmt.Println(">>>>>>>> Add Table")
////	for _,v := range BASE64_TABLES {
////		stub.Args = []string{"add", collectionKey, op, v}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////	}
////	fmt.Println(">>>>>>>> All Table")
////	var tables []db.Table
////	{
////		stub.Args = []string{"all",collectionKey, op}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////		fmt.Println(string(response.Payload))
////		err := json.Unmarshal(response.Payload, &tables); if err != nil {
////			panic(err)
////		}
////	}
////	fmt.Println(">>>>>>>> Update Table")
////	for _,table := range tables {
////		table.Columns = append(table.Columns, db.Column{"test",db.INT,"",false,"test"})
////		tableJson,err := t.EncodeJson(table); if err != nil {
////			panic(err)
////		}
////		stub.Args = []string{"update", collectionKey, op, tableJson}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////	}
////	fmt.Println(">>>>>>>> Get Table")
////	for _,table := range tables {
////		stub.Args = []string{"get", collectionKey, op, table.Name}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////		fmt.Println(string(response.Payload))
////	}
////	fmt.Println(">>>>>>>> History Table")
////	for _,table := range tables {
////		stub.Args = []string{"history", collectionKey, op, table.Name, "2"}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////		fmt.Println(string(response.Payload))
////	}
////
////	///////////////// schema //////////////////
////	op = "schema"
////	fmt.Println(">>>>>>>> Set Schema(Add)")
////	for _,v := range BASE64_SCHEMAS {
////		stub.Args = []string{"add", collectionKey, op, v}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////	}
////	fmt.Println(">>>>>>>> All Schema")
////	var schemas []db.Schema
////	{
////		stub.Args = []string{"all", collectionKey, op}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////		fmt.Println(string(response.Payload))
////		err := json.Unmarshal(response.Payload, &schemas); if err != nil {
////		panic(err)
////	}
////	}
////	fmt.Println(">>>>>>>> Set Schema(Update)")
////	for _,schema := range schemas {
////		schema.Model.Models = append(schema.Model.Models, db.Model{"updateCollectionFields",COLLECTION_FIELD_TABLE.Name,true,[]db.Model{}})
////		tableJson,err := t.EncodeJson(schema); if err != nil {
////			panic(err)
////		}
////		stub.Args = []string{"update", collectionKey, op, tableJson}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////	}
////	fmt.Println(">>>>>>>> Get Schema")
////	for _,schema := range schemas {
////		stub.Args = []string{"get", collectionKey, op, schema.Name}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////		fmt.Println(string(response.Payload))
////	}
////	fmt.Println(">>>>>>>> History Schema")
////	for _,schema := range schemas {
////		stub.Args = []string{"history", collectionKey, op, schema.Name, "2"}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////		fmt.Println(string(response.Payload))
////	}
////
////	///////////////// tableRow //////////////////
////	op = "tableRow"
////	fmt.Println(">>>>>>>> Set TableRow(Add)")
////	for _,table := range tables {
////		fmt.Println("Table:" + table.Name)
////		rowDemo,err := t.QueryRowDemo(stub, table.Name)
////		if err != nil {
////			panic(err)
////		}
////		rowJson,err := t.EncodeJson(rowDemo); if err != nil {
////			panic(err)
////		}
////		stub.Args = []string{"add", collectionKey, op, table.Name, rowJson}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////		fmt.Println("RowID:" + string(response.Payload))
////	}
////	fmt.Println(">>>>>>>> Set TableRow(Update)")
////	for _,table := range tables {
////		fmt.Println("Table:" + table.Name)
////		rowDemo,err := t.QueryRowDemo(stub, table.Name)
////		if err != nil {
////			panic(err)
////		}
////		for _,column := range table.Columns {
////			var updateData interface{}
////			match,_ := t.MatchForeignKeyByKey(table.ForeignKeys, column.Name)
////			if column.Name == table.PrimaryKey.Column || match {
////				updateData = 1
////			}else{
////				switch column.Type {
////				case db.VARCHAR:
////					updateData = "update"
////				case db.INT:
////					updateData = 2
////				case db.DECIMAL:
////					updateData = "2222222.222222222222"
////				case db.BOOL:
////					updateData = true
////				}
////			}
////			rowDemo[column.Name] = updateData
////		}
////		rowJson,err := t.EncodeJson(rowDemo); if err != nil {
////			panic(err)
////		}
////		stub.Args = []string{"update", collectionKey, op, table.Name, "1", rowJson}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////		fmt.Println("RowID:" + string(response.Payload))
////	}
////	fmt.Println(">>>>>>>> Get TableRow")
////	for _,table := range tables {
////		fmt.Println("Table:" + table.Name)
////		stub.Args = []string{"get", collectionKey, op, table.Name, "1", "15"}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////		fmt.Println("Rows:" + string(response.Payload))
////	}
////	fmt.Println(">>>>>>>> History TableRow")
////	for _,table := range tables {
////		fmt.Println("Table:" + table.Name)
////		stub.Args = []string{"history", collectionKey, op, table.Name, "1", "2"}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////		fmt.Println("Rows:" + string(response.Payload))
////	}
////
////	///////////////// schemaRow //////////////////
////	op = "schemaRow"
////	fmt.Println(">>>>>>>> Set SchemaRow(Add)")
////	schemaIds := map[string][]string{}
////	for _,schema := range schemas {
////		fmt.Println("Schema:" + schema.Name)
////		rowDemo,err := t.QuerySchemaRowDemo(stub, schema.Name)
////		if err != nil {
////			panic(err)
////		}
////		rowJson,err := t.EncodeJson(rowDemo); if err != nil {
////			panic(err)
////		}
////		stub.Args = []string{"add", collectionKey, op, schema.Name, rowJson}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////		rowIds := string(response.Payload)
////		var ids []string
////		err = json.Unmarshal(response.Payload, &ids)
////		if err != nil {
////			panic(err)
////		}
////		schemaIds[schema.Name] = ids
////		fmt.Println("RowIDs:" + rowIds)
////	}
////	fmt.Println(">>>>>>>> Set SchemaRow(Update)")
////	for _,schema := range schemas {
////		fmt.Println("Schema:" + schema.Name)
////		var rows []interface{}
////		rowIds := schemaIds[schema.Name]
////		for _,id := range rowIds {
////			row,err := t.QuerySchemaRow(stub, schema.Name, id)
////			if err != nil {
////				panic(err)
////			}
////			table,err := t.QueryTable(stub, schema.Model.Table); if err != nil {
////				panic(err)
////			}
////			for _,column := range table.Columns {
////				var updateData interface{}
////				match,_ := t.MatchForeignKeyByKey(table.ForeignKeys, column.Name)
////				if column.Name == table.PrimaryKey.Column || match {
////				}else{
////					switch column.Type {
////					case db.VARCHAR:
////						updateData = "update"
////					case db.INT:
////						updateData = 2
////					case db.DECIMAL:
////						updateData = "2222222.222222222222"
////					case db.BOOL:
////						updateData = true
////					}
////				}
////				row[column.Name] = updateData
////			}
////			rows = append(rows, row)
////		}
////		var schemaRow interface{}
////		if schema.Model.IsArray {
////			schemaRow = rows
////		}else{
////			schemaRow = rows[0]
////		}
////		rowJson,err := t.EncodeJson(schemaRow); if err != nil {
////			panic(err)
////		}
////		stub.Args = []string{"update", collectionKey, op, schema.Name, "1", rowJson}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////		fmt.Println("RowIDs:" + string(response.Payload))
////	}
////	fmt.Println(">>>>>>>> Get SchemaRow")
////	for _,schema := range schemas {
////		fmt.Println("Schema:" + schema.Name)
////		stub.Args = []string{"get", collectionKey, op, schema.Name, "1", "15"}
////		stub.TxID = GetTxID()
////		response := database.Invoke(stub)
////		if response.Status != shim.OK {
////			panic(response.Message)
////		}
////		fmt.Println("Rows:" + string(response.Payload))
////	}
////}
//
//func GetTxID() string {
//	TXID_NUM++
//	zeroNum := ""
//	if TXID_NUM < 10 {
//		zeroNum = "000"
//	}else if TXID_NUM < 100 {
//		zeroNum = "00"
//	}else if TXID_NUM < 1000 {
//		zeroNum = "0"
//	}
//	return fmt.Sprintf("53b77e1cfb27378d5a82264f24a04a9ba528fca91c1604c7adee711f4746%s%d", zeroNum, TXID_NUM)
//}

//func TestData(te *testing.T) {
//	var t = new(db.DbManager)
//	var stub = new(test.TestChaincodeStub)
//	collectionKey := "collection_org1"
//	stub.Args = []string{ collectionKey }
//	for _,v := range TABLES {
//		fmt.Println("========="+v.Name+"=========")
//		//fmt.Println("CreateTable")
//		if err := CreateTable(t, stub, v); err != nil {
//			panic(err)
//		}
//
//		//fmt.Println("QueryTable")
//		table,err := t.QueryTable(stub, v.Name)
//		if err != nil {
//			panic(err)
//		}
//		tableByte,err := t.ConvertJsonBytes(table)
//		fmt.Println(string(tableByte))
//
//		fmt.Println("QueryRowDemo")
//		rowDemo,err := t.QueryRowDemo(stub, v.Name)
//		if err != nil {
//			panic(err)
//		}
//		rbyte,err := t.ConvertJsonBytes(rowDemo)
//		fmt.Println(string(rbyte))
//
//		var row map[string]interface{}
//		if err := json.Unmarshal(rbyte, &row); err != nil {
//			panic(err)
//		}
//
//		for i:=0;i<10;i++ {
//			fmt.Println("SetRow")
//			ids,err := t.SetRow(stub, v.Name, []db.TableRow{{"",row}}, db.ADD)
//			if err != nil {
//				panic(err)
//			}
//
//			//fmt.Println("GetTabledb.PrimaryKey")
//			//idKey := t.GetTabledb.PrimaryKey(v)
//			//id := t.ConvertString(row[idKey])
//
//			fmt.Println("QueryRow")
//			qrbyte,err := t.QueryRowBytes(stub, v.Name, ids[0])
//			if err != nil {
//				panic(err)
//			}
//			if len(qrbyte) > 0 {
//				fmt.Println(string(qrbyte))
//			}
//		}
//
//		fmt.Println("QueryRowWithPagination")
//		paginationBytes,err := t.QueryRowWithPaginationBytes(stub, v.Name, "", 10)
//		if err != nil {
//			panic(err)
//		}
//		var pagination db.Pagination
//		err = json.Unmarshal(paginationBytes, &pagination); if err != nil {
//			panic(err)
//		}
//		if len(pagination.List) > 0 {
//			qrbyte,_ := t.ConvertJsonBytes(pagination)
//			fmt.Println(string(qrbyte))
//		}
//	}
//
//	id,err := t.SetRow(stub, CATEGORY_TABLE.Name,[]db.TableRow{{"10",map[string]interface{}{
//		"name": "",
//	}}}, db.UPDATE)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println(id)
//
//	id1,err := t.SetRow(stub, CATEGORY_TABLE.Name,[]db.TableRow{{"", map[string]interface{}{
//		"id":nil,
//		"name": "","create_at":"","update_at":"",
//	}}}, db.ADD)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println(id1)
//
//	fmt.Println("SetSchema")
//	schemaJson,err := t.ConvertJsonString(COLLECTION_SCHEMA); if err != nil {
//		panic(err)
//	}
//	_,err = t.AddSchemaByJson(stub, schemaJson); if err != nil {
//		panic(err)
//	}
//
//	fmt.Println("QuerySchema")
//	qrbyte,err := t.QuerySchemaBytes(stub, COLLECTION_SCHEMA.Name); if err != nil {
//		panic(err)
//	}
//	fmt.Println(string(qrbyte))
//
//	fmt.Println("QuerySchemaRowDemo")
//	schemaRow,err := t.QuerySchemaRowDemo(stub, COLLECTION_SCHEMA.Name); if err != nil {
//		panic(err)
//	}
//	srbyte,_ := t.ConvertJsonBytes(schemaRow)
//	schemaRowJson := string(srbyte)
//	fmt.Println(schemaRowJson)
//	fmt.Println("SetSchemaRow")
//	schemaRowIds,schemaRows,err := t.AddSchemaRowByJson(stub, COLLECTION_SCHEMA.Name, schemaRowJson); if err != nil {
//		panic(err)
//	}
//	fmt.Println(schemaRowIds)
//	srsbyte,_ := t.ConvertJsonBytes(schemaRows)
//	fmt.Println(string(srsbyte))
//
//	fmt.Println("QuerySchemaRow")
//	querySchemaRow,err := t.QuerySchemaRow(stub, COLLECTION_SCHEMA.Name, schemaRowIds[0]); if err != nil {
//		panic(err)
//	}
//	qsrbyte,_ := t.ConvertJsonBytes(querySchemaRow)
//	fmt.Println(string(qsrbyte))
//
//	fmt.Println("QuerySchemaRowByWithPaginationBytes")
//	querySchemaRowBytes,err := t.QuerySchemaRowByWithPaginationBytes(stub, COLLECTION_SCHEMA.Name, schemaRowIds[0], 2); if err != nil {
//		panic(err)
//	}
//	fmt.Println(string(querySchemaRowBytes))
//
//	fmt.Println("QueryRow")
//	rowDemo,err := t.QueryRowDemo(stub, COLLECTION_SCHEMA.Model.Table)
//	if err != nil {
//		panic(err)
//	}
//	ids2,err := t.SetRow(stub, COLLECTION_SCHEMA.Model.Table,[]db.TableRow{{"", rowDemo}}, db.ADD)
//	if err != nil {
//		panic(err)
//	}
//	querySchemaRow2,err := t.QuerySchemaRow(stub, COLLECTION_SCHEMA.Name, ids2[0]); if err != nil {
//		panic(err)
//	}
//	qsrbyte2,_ := t.ConvertJsonBytes(querySchemaRow2)
//	fmt.Println(string(qsrbyte2))
//
//
//	fmt.Println("QuerySchemaRow UpdateRow")
//	updateRowDemo,err := t.QueryRowDemo(stub, COLLECTION_FIELD_TABLE.Name)
//	if err != nil {
//		panic(err)
//	}
//	updateIds,err := t.SetRow(stub, COLLECTION_FIELD_TABLE.Name,[]db.TableRow{{"", updateRowDemo}}, db.ADD)
//	if err != nil {
//		panic(err)
//	}
//	updateRowDemo[COLLECTION_FIELD_TABLE.ForeignKeys[0].Column] = ids2[0]
//	updateIds,err = t.SetRow(stub, COLLECTION_FIELD_TABLE.Name, []db.TableRow{{updateIds[0], updateRowDemo}}, db.UPDATE)
//	if err != nil {
//		panic(err)
//	}
//	querySchemaRow2,err = t.QuerySchemaRow(stub, COLLECTION_SCHEMA.Name, ids2[0]); if err != nil {
//		panic(err)
//	}
//	qsrbyte2,_ = t.ConvertJsonBytes(querySchemaRow2)
//	fmt.Println(string(qsrbyte2))
//
//
//	///////////////// history //////////////////
//	fmt.Println("History Operation")
//	fmt.Println("Table")
//	qsrbyte3,err := t.QueryTableHistoryBytes(stub, COLLECTION_FIELD_TABLE.Name,0); if err != nil {
//		panic(err)
//	}
//	fmt.Println(string(qsrbyte3))
//
//	fmt.Println("Schema")
//	qsrbyte4,err := t.QuerySchemaHistoryBytes(stub, COLLECTION_SCHEMA.Name,0); if err != nil {
//		panic(err)
//	}
//	fmt.Println(string(qsrbyte4))
//
//	fmt.Println("Row")
//	qsrbyte5,err := t.QueryRowHistoryBytes(stub, COLLECTION_FIELD_TABLE.Name, updateIds[0],0); if err != nil {
//		panic(err)
//	}
//	fmt.Println(string(qsrbyte5))
//
//
//	//fmt.Println("StubChainCodeData")
//	//qrbyte,_ := t.ConvertJsonBytes(stub.data)
//	//fmt.Println(string(qrbyte))
//
//
//
//
//	////解密数据
//	//jsonData,err := t.DecryptData(VALUE)
//	//if err != nil {
//	//	fmt.Println(err)
//	//	return
//	//}
//	//var rows []Row
//	//rows,err = t.RecursionJsonData(GetTestTable, nil, COLLECTION_SCHEMA.LayerNum,0,"", jsonData, COLLECTION_SCHEMA.Model, rows)
//	//if err != nil {
//	//	fmt.Println(err)
//	//	return
//	//}
//	//fmt.Println("verify success")
//}

//func CreateTable(t *db.DbManager,stub shim.ChaincodeStubInterface, table db.Table) error {
//	tableJson,err := t.ConvertJsonString(table); if err != nil {
//		return err
//	}
//
//	if _,err := t.AddTableByJson(stub, tableJson); err != nil {
//		return err
//	}
//	return nil
//}