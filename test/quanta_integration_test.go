package test

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"testing"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr/builtins"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/araddon/qlbridge/schema"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/custom/functions"
	"github.com/disney/quanta/rbac"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type QuantaTestSuite struct {
	suite.Suite
	node  *server.Node
	store *shared.KVStore
}

func (suite *QuantaTestSuite) SetupSuite() {
	var err error
	suite.node, err = Setup() // harness setup
	assert.NoError(suite.T(), err)

	core.ClearTableCache()
	RemoveContents("./testdata/index")
	RemoveContents("./testdata/bitmap")

	// Server side components already started and available in package level variables in harness.go
	conn := shared.NewDefaultConnection()
	conn.ServicePort = 0
	err = conn.Connect(nil) // no consul
	assert.NoError(suite.T(), err)

	u.SetupLogging("debug")


	// load all of our built-in functions
	builtins.LoadAllBuiltins()
	functions.LoadAll() // Custom functions

	// Simulate the mySQL proxy endpoint for golang dbdriver connection clients.
	src, err2 := source.NewQuantaSource("./testdata/config", "", 0, 1)
	assert.NoError(suite.T(), err2)
	schema.RegisterSourceAsSchema("quanta", src)

	suite.store = shared.NewKVStore(conn)
	assert.NotNil(suite.T(), suite.store)

	ctx, err := rbac.NewAuthContext(suite.store, "USER001", true)
	assert.NoError(suite.T(), err)
	err = ctx.GrantRole(rbac.DomainUser, "USER001", "quanta", true)
	assert.NoError(suite.T(), err)

	// load up vision test data (nested schema containing 3 separate tables)
	suite.loadData("cities", "./testdata/us_cities.parquet", conn, false)
	suite.loadData("cityzip", "./testdata/us_cityzip.parquet", conn, false)
	//suite.insertData("cityzip", "./testdata/us_cityzip.parquet")
	// suite.loadData("nba", "./testdata/nba.parquet")

}

func (suite *QuantaTestSuite) loadData(table, filePath string, conn *shared.Conn, ignoreSourcePath bool) error {

	fr, err := local.NewLocalFileReader(filePath)
	assert.Nil(suite.T(), err)
	if err != nil {
		log.Println("Can't open file", err)
		return err
	}
	pr, err := reader.NewParquetColumnReader(fr, 4)
	assert.Nil(suite.T(), err)
	if err != nil {
		log.Println("Can't create column reader", err)
		return err
	}
	num := int(pr.GetNumRows())
	c, err := core.OpenSession("./testdata/config", table, false, conn)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c)

	for i := 1; i <= num; i++ {
		err := c.PutRow(table, pr, 0, ignoreSourcePath)
		assert.Nil(suite.T(), err)
	}
	c.Flush()
	c.CloseSession()
	return nil
}

func (suite *QuantaTestSuite) insertData(table, filePath string) error {

	// Connect using GoLang database/sql driver.
	db, err := sql.Open("qlbridge", "quanta")
	defer db.Close()
	assert.NoError(suite.T(), err)

	// Set user id in session
	setter := "set @userid = 'USER001'"
	_, err = db.Exec(setter)
	assert.NoError(suite.T(), err)

    insertStmt, err := db.Prepare("insert into cityzip (id, zip, city, state) values (?, ?, ?, ?)")
	assert.NoError(suite.T(), err)
    defer insertStmt.Close()

	fr, err := local.NewLocalFileReader(filePath)
	assert.Nil(suite.T(), err)
	if err != nil {
		log.Println("Can't open file", err)
		return err
	}
	pr, err := reader.NewParquetReader(fr, nil,  4)
	assert.Nil(suite.T(), err)
	if err != nil {
		log.Println("Can't create column reader", err)
		return err
	}
	num := int(pr.GetNumRows())
	res, err := pr.ReadByNumber(num)
	if err != nil {
		log.Println("Can't read", err)
		return err
	}

	for _, v := range res {
		x := v.(struct{Id string; Zip string; Name string; State string})
		_, err := insertStmt.Exec(x.Id, x.Zip, x.Name, x.State)
		assert.NoError(suite.T(), err)
	}

	pr.ReadStop()
	fr.Close()
	return nil
}
func (suite *QuantaTestSuite) runQuery(q string, args []interface{}) ([]string, []string, error) {

	// Connect using GoLang database/sql driver.
	db, err := sql.Open("qlbridge", "quanta")
	defer db.Close()
	if err != nil {
		return []string{""}, []string{""}, err
	}

	// Set user id in session
	setter := "set @userid = 'USER001'"
	_, err = db.Exec(setter)
	assert.NoError(suite.T(), err)

	log.Printf("EXECUTING SQL: %v", q)
	var rows *sql.Rows
	var stmt *sql.Stmt
	if args != nil {
		stmt, err = db.Prepare(q)
		assert.NoError(suite.T(), err)
		rows, err = stmt.Query(args...)
	} else {
		rows, err = db.Query(q)
	}
	if err != nil {
		return []string{""}, []string{""}, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()

	// this is just stupid hijinx for getting pointers for unknown len columns
	readCols := make([]interface{}, len(cols))
	writeCols := make([]string, len(cols))
	for i := range writeCols {
		readCols[i] = &writeCols[i]
	}
	results := make([]string, 0)
	for rows.Next() {
		rows.Scan(readCols...)
		result := strings.Join(writeCols, ",")
		results = append(results, result)
		log.Println(result)
	}
	log.Println("")
	return results, cols, nil
}

func (suite *QuantaTestSuite) runDML(q string, args []interface{}) (error, int64) {

	assert.NotZero(suite.T(), q)

	// Connect using GoLang database/sql driver directly to Quanta engine
	db, err := sql.Open("qlbridge", "quanta")
	defer db.Close()
	if err != nil {
		return err, 0
	}

	// Set user id in session
	setter := "set @userid = 'USER001'"
	_, err = db.Exec(setter)
	assert.NoError(suite.T(), err)

	log.Printf("EXECUTING DML: %v", q)

	var stmt *sql.Stmt
	var result sql.Result
	if args != nil && len(args) > 0 {
		log.Printf("PREPARING: %v", q)
		stmt, err = db.Prepare(q)
		log.Printf("PREPARED: %#v", stmt)
		assert.NoError(suite.T(), err)
		log.Printf("EXEC: %#v", args)
		result, err = stmt.Exec(args...)
		assert.NoError(suite.T(), err)
	} else {
		result, err = db.Exec(q)
		assert.NoError(suite.T(), err)
	}
	count, _ := result.RowsAffected()
	return nil, count
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestQuantaTestSuite(t *testing.T) {
	suite.Run(t, new(QuantaTestSuite))
}

// Test count query with nested data source
func (suite *QuantaTestSuite) TestSimpleQuery() {
	results, _, err := suite.runQuery("select count(*) from cities", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("29488", results[0])
}

// Test projection with nested data source
func (suite *QuantaTestSuite) TestSimpleProjection() {
	results, _, err := suite.runQuery("select id, name, state_name, state from cities limit 100000", nil)
	assert.NoError(suite.T(), err)
	suite.Equal(29488, len(results))
}

// Test inner join with nested data source
func (suite *QuantaTestSuite) TestInnerJoin() {
	results, _, err := suite.runQuery("select count(*) from cityzip as z inner join cities as c on c.id = z.city_id", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("46280", results[0])
}

// Test anti-join
func (suite *QuantaTestSuite) TestAntiJoin() {
	results, _, err := suite.runQuery("select count(*) from cityzip as z inner join cities as c on c.id != z.city_id where z.city = 'Oceanside'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("46270", results[0])
	results, _, err = suite.runQuery("select c.id, c.name, c.state_name, c.state from cityzip as z inner join cities as c on c.id != z.city_id where z.city != 'Oceanside' limit 100000", nil)
	assert.NoError(suite.T(), err)
	suite.Equal(10, len(results))
}

// Test outer join
func (suite *QuantaTestSuite) TestOuterJoinWithPredicate() {
	results, _, err := suite.runQuery("select count(*) from cityzip as z outer join cities as c on c.id = z.city_id where z.city = 'Oceanside'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("10", results[0])
	results, _, err = suite.runQuery("select c.id, c.name, c.state_name, c.state from cityzip as z outer join cities as c on c.id = z.city_id where z.city = 'Oceanside'", nil)
	assert.NoError(suite.T(), err)
	suite.Equal(10, len(results))
}

// Test outer join no predicate
func (suite *QuantaTestSuite) TestOuterJoinNoPredicate() {
	results, _, err := suite.runQuery("select count(*) from cities as c outer join cityzip as z on c.id = z.city_id", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("46280", results[0])
}

func (suite *QuantaTestSuite) TestSQLSyntaxUnknownKeyword() {
	_, _, err := suite.runQuery("selectX count(*) from cities", nil)
	assert.EqualError(suite.T(), err, "Unrecognized request type: selectX")
}

func (suite *QuantaTestSuite) TestSQLSyntaxUnknownTable() {
	_, _, err := suite.runQuery("select count(*) from citiesx", nil)
	assert.EqualError(suite.T(), err, "QLBridge.plan: No datasource found")
}

func (suite *QuantaTestSuite) TestSQLSyntaxUnknownField() {
	_, _, err := suite.runQuery("select count(*) from cities where nonsensefield = null", nil)
	assert.Error(suite.T(), err)
}

func (suite *QuantaTestSuite) TestBetweenWithNegative() {
	results, _, err := suite.runQuery("select count(*) from cities where latitude BETWEEN 41.0056 AND 44.9733 AND longitude BETWEEN '-111.0344' AND '-104.0692'", nil)

	assert.NoError(suite.T(), err)
	suite.Equal("202", results[0]) // Count of all cities in WY
}

func (suite *QuantaTestSuite) TestNotBetween() {
	results, _, err := suite.runQuery("select count(*) from cities where population NOT BETWEEN 100000 and 150000", nil)
	assert.NoError(suite.T(), err)
	suite.Equal("29321", results[0])
}

// Test subquery join
func (suite *QuantaTestSuite) TestSubqueryJoin() {
	results, _, err := suite.runQuery("select count(*) from cities where id in (select city_id from cityzip where city = 'Oceanside')", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("10", results[0])

	results, _, err = suite.runQuery("select count(*) from cities where id not in (select city_id from cityzip where city = 'Oceanside')", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("46270", results[0])
	results, _, err = suite.runQuery("select id, name, state_name, state from cities where id in (select city_id from cityzip where city = 'Oceanside')", nil)
	assert.NoError(suite.T(), err)
	suite.Equal(10, len(results))
}

func (suite *QuantaTestSuite) TestInvalidTableOnJoin() {
	_, _, err := suite.runQuery("select count(*) from cities as c inner join faketable as f on c.id = f.fake_id", nil)
	assert.EqualError(suite.T(), err, "invalid table faketable in join criteria [INNER JOIN faketable AS f ON c.id = f.fake_id]")
}

func (suite *QuantaTestSuite) TestInvalidFieldOnJoin() {
	_, _, err := suite.runQuery("select count(*) from cities as c inner join cityzip as z on c.id = z.fake_field", nil)
	assert.EqualError(suite.T(), err, "invalid field fake_field in join criteria [INNER JOIN cityzip AS z ON c.id = z.fake_field]")
}

func (suite *QuantaTestSuite) TestSelectStar() {
	results, _, err := suite.runQuery("select * from cities where timezone != NULL limit 100000 with timeout=500", nil)
	assert.NoError(suite.T(), err)
	suite.Equal(29488, len(results))
}

func (suite *QuantaTestSuite) TestSelectStarWithAlias() {
	results, _, err := suite.runQuery("select count(*) from cities as c", nil)
	assert.NoError(suite.T(), err)
	suite.Equal("29488", results[0])
}

func (suite *QuantaTestSuite) TestTableAlias() {
	results, cols, err := suite.runQuery("select a.id as xid, a.state_name as xstate_name, a.state as xstate from cities as a where a.state_name = 'New York State' and a.state = 'NY'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(1186, len(results))
	suite.Equal(3, len(cols))
	suite.Equal("xid", cols[0])
	suite.Equal("xstate_name", cols[1])
	suite.Equal("xstate", cols[2])
}

func (suite *QuantaTestSuite) TestJoinWithoutOnClause() {
	_, _, err := suite.runQuery("select count(*) from cities as c inner join cityzip as z", nil)
	assert.EqualError(suite.T(), err, "join criteria missing (ON clause)")
}

func (suite *QuantaTestSuite) TestJoinWithNonkeyFields() {
	_, _, err := suite.runQuery("select count(*) from cities as c inner join cityzip as z on c.state_name = z.state", nil)
	assert.EqualError(suite.T(), err, "join field state_name is not a relation")
}

// AGGREGATES
// SUM, MIN, MAX, AVERAGE
func (suite *QuantaTestSuite) TestSumInvalidFieldName() {
	_, _, err := suite.runQuery("select sum(foobar) from cities WHERE timezone != NULL", nil)
	assert.EqualError(suite.T(), err, "cannot resolve field cities.foobar in sum() function argument")
}

func (suite *QuantaTestSuite) TestSumInvalidFieldType() {
	_, _, err := suite.runQuery("select sum(state_name) from cities", nil)
	assert.EqualError(suite.T(), err, "can't sum a non-bsi field state_name")
}

func (suite *QuantaTestSuite) TestSimpleSum() {
	results, _, err := suite.runQuery("select sum(population) from cities", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("406795495", results[0])
}

func (suite *QuantaTestSuite) TestSimpleAvg() {
	results, _, err := suite.runQuery("select avg(population) from cities", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("13795", results[0])
}

func (suite *QuantaTestSuite) TestAvgInvalidFieldName() {
	_, _, err := suite.runQuery("select avg(foobar) from cities WHERE timezone != NULL", nil)
	assert.EqualError(suite.T(), err, "cannot resolve field cities.foobar in avg() function argument")
}

func (suite *QuantaTestSuite) TestAvgInvalidFieldType() {
	_, _, err := suite.runQuery("select avg(state_name) from cities", nil)
	assert.EqualError(suite.T(), err, "can't average a non-bsi field state_name")
}

func (suite *QuantaTestSuite) TestSimpleMin() {
	results, _, err := suite.runQuery("select min(population) from cities where name = 'Oceanside'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	//suite.Equal("2530", results[0])
	suite.Equal("       352", results[0])
}

func (suite *QuantaTestSuite) TestNegativeMin() {
	results, _, err := suite.runQuery("select min(longitude) from cities where state = 'WY'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(" -111.0344", results[0])
}

func (suite *QuantaTestSuite) TestMinInvalidFieldName() {
	_, _, err := suite.runQuery("select min(foobar) from cities WHERE timezone != NULL", nil)
	assert.EqualError(suite.T(), err, "cannot resolve field cities.foobar in min() function argument")
}

func (suite *QuantaTestSuite) TestMinInvalidFieldType() {
	_, _, err := suite.runQuery("select min(state_name) from cities", nil)
	assert.EqualError(suite.T(), err, "can't find the minimum of a non-bsi field state_name")
}

func (suite *QuantaTestSuite) TestSimpleMax() {
	results, _, err := suite.runQuery("select max(population) from cities", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("  18713220", results[0])
}

func (suite *QuantaTestSuite) TestMaxInvalidFieldName() {
	_, _, err := suite.runQuery("select max(foobar) from cities WHERE timezone != NULL", nil)
	assert.EqualError(suite.T(), err, "cannot resolve field cities.foobar in max() function argument")
}

func (suite *QuantaTestSuite) TestMaxInvalidFieldType() {
	_, _, err := suite.runQuery("select max(state_name) from cities", nil)
	assert.EqualError(suite.T(), err, "can't find the maximum of a non-bsi field state_name")
}

func (suite *QuantaTestSuite) TestSimpleRank() {
	results, _, err := suite.runQuery("select topn(state_name) from cities", nil)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 53, len(results))
}

// END AGGREGATES

func (suite *QuantaTestSuite) TestCityzipCount() {
	results, _, err := suite.runQuery("select count(*) from cityzip", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("46280", results[0])
}

func (suite *QuantaTestSuite) TestCitiesRegionList() {
	results, _, err := suite.runQuery("select count(*) from cities where region_list != null", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("29488", results[0])
	results, _, err = suite.runQuery("select count(*) from cities where region_list = 'NY'", nil)
	assert.NoError(suite.T(), err)
	suite.Equal("1186", results[0])
}

func (suite *QuantaTestSuite) TestCitiesTimestamp() {
	results, _, err := suite.runQuery("select created_timestamp from cities", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(5000, len(results))
	// for _, v := range results {
	// 	assert.True(suite.T(), strings.HasPrefix(v, "1970-01-16"))
	// }
}

func (suite *QuantaTestSuite) TestCitiesIntDirect() {
	results, _, err := suite.runQuery("select count(*) from cities where ranking = 1", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("50", results[0])
}

func (suite *QuantaTestSuite) TestCitiesBoolDirect() {
	results, _, err := suite.runQuery("select count(*) from cities where military = true", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("84", results[0])
	results, _, err = suite.runQuery("select count(*) from cities where military = 1", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("84", results[0])
}

// Like statements
// LIKE will index based off a whole word (separated by whitespace)
// Wildcard operators are not necessary
func (suite *QuantaTestSuite) TestCitiesLikeStatement() {
	results, _, err := suite.runQuery("select county, name from cities where name like 'woods' and name like 'HAWTHORN'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(1, len(results))
}

func (suite *QuantaTestSuite) TestCitiesLikeListStatement() {
	_, _, err := suite.runQuery("select count(*) from cities where region_list like 'NY'", nil)
	assert.EqualError(suite.T(), err, "LIKE operator not supported for non-range field 'region_list'")
}

// DISTINCT statement
func (suite *QuantaTestSuite) TestCitiesDistinctStatement() {
	results, _, err := suite.runQuery("select distinct state_name from cities", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(52, len(results))
}

// AND conditional statement
func (suite *QuantaTestSuite) TestCitiesAndWhereStatement() {
	results, _, err := suite.runQuery("select id, state_name, state from cities where state_name = 'New York State' and state = 'NY'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(1186, len(results))
}

// OR conditional statement
func (suite *QuantaTestSuite) TestCitiesOrWhereStatement() {
	results, _, err := suite.runQuery("select id, state_name, state from cities where state_name = 'New York State' or state = 'NY'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(1186, len(results))
}

// AND / OR conditional statement
func (suite *QuantaTestSuite) TestCitiesAndOrWhereStatement() {
	results, _, err := suite.runQuery("select id, state_name, state from cities where state_name = 'New York State' or state = 'NY' and ranking = 3", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(1051, len(results))
}

// IN statements
func (suite *QuantaTestSuite) TestCitiesINStatement() {
	results, _, err := suite.runQuery("select count(*) from cities where state IN ('NY','AK','WA')", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("2154", results[0])
}

// IN with NOT statement
func (suite *QuantaTestSuite) TestCitiesNotINStatement() {
	results, _, err := suite.runQuery("select count(*) from cities where state not IN ('NY','AK','WA')", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("27334", results[0])
}

// NOT with OR
func (suite *QuantaTestSuite) TestCitiesNotWithOR() {
	results, _, err := suite.runQuery("select count(*) from cities where county != 'Nassau' or state != 'WA'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	//suite.Equal("28866", results[0])
	suite.Equal("29488", results[0])
}

func (suite *QuantaTestSuite) TestCitiesNotWithAND() {
	results, _, err := suite.runQuery("select count(*) from cities where county != 'Mason' and state = 'WA' and military != true and population > 0", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("596", results[0])
}

// SELECT List Functions
func (suite *QuantaTestSuite) TestSimpleSelectListFunction() {
	results, _, err := suite.runQuery("select id, add(population + 10) from cities where state = 'NY' and name = 'Oceanside'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("1840005246,31195", results[0])
}

func (suite *QuantaTestSuite) TestSelectListFunctionInvalidArgument() {
	_, _, err := suite.runQuery("select id, add(foobar + 10) from cities WHERE timezone != NULL", nil)
	assert.EqualError(suite.T(), err, "cannot resolve field cities.foobar in add() function argument")
}

func (suite *QuantaTestSuite) TestSelectListFunctionInvalidFunction() {
	_, _, err := suite.runQuery("select id, foobar(population + 10) from cities WHERE timezone != NULL", nil)
	assert.EqualError(suite.T(), err, "func \"foobar\" not found while processing select list")
}

func (suite *QuantaTestSuite) TestZDropTables() {
	err := suite.store.DeleteIndicesWithPrefix("cityzip", false)
	assert.Nil(suite.T(), err)
	err = suite.store.DeleteIndicesWithPrefix("cities", true)
	assert.Nil(suite.T(), err)
}

func (suite *QuantaTestSuite) TestXDML1Insert() {
	err, count := suite.runDML("insert into dmltest (name, age, gender) values ('Tom', 20, 'M')", nil)
	suite.Equal(int64(1), count)
	assert.Nil(suite.T(), err)
	results, _, err2 := suite.runQuery("select count(*) from dmltest", nil)
	assert.NoError(suite.T(), err2)
	suite.Equal("1", results[0])
}

func (suite *QuantaTestSuite) TestXDML2Update() {
	results, _, err := suite.runQuery("select date, name, age, gender from dmltest", nil)
	assert.Nil(suite.T(), err)
	values := strings.Split(results[0], ",")
	qry := fmt.Sprintf("select count(*) from dmltest where date = '%s' and name = '%s'", values[0], values[1])
	results, _, err2 := suite.runQuery(qry, nil)
	assert.NoError(suite.T(), err2)
	suite.Equal("1", results[0])
	upd := fmt.Sprintf("update dmltest set age = 21, gender = 'U' where date = '%s' and name = '%s'", values[0], values[1])
	err, _ = suite.runDML(upd, nil)
	assert.Nil(suite.T(), err)
	results, _, err = suite.runQuery("select date, name, age, gender from dmltest", nil)
	assert.Nil(suite.T(), err)
	suite.Equal(1, len(results))
	values = strings.Split(results[0], ",")
	suite.Equal(4, len(values))
	suite.Equal("21", strings.TrimSpace(values[2]))
	suite.Equal("U", strings.TrimSpace(values[3]))
}

func (suite *QuantaTestSuite) TestXDML2UpdatePrepared() {
	results, _, err := suite.runQuery("select date, name, age, gender from dmltest", nil)
	assert.Nil(suite.T(), err)
	values := strings.Split(results[0], ",")
	qry := fmt.Sprintf("select count(*) from dmltest where date = ? and name = ?")
	vals := make([]interface{}, 2)
	vals[0] = values[0]
	vals[1] = values[1]
	results, _, err2 := suite.runQuery(qry, vals)
	assert.NoError(suite.T(), err2)
	suite.Equal("1", results[0])
	upd := fmt.Sprintf("update dmltest set age = 21, gender = 'U' where date = ? and name = ?")
	err, _ = suite.runDML(upd, vals)
	assert.Nil(suite.T(), err)
	results, _, err = suite.runQuery("select date, name, age, gender from dmltest", nil)
	assert.Nil(suite.T(), err)
	suite.Equal(1, len(results))
	values = strings.Split(results[0], ",")
	suite.Equal(4, len(values))
	suite.Equal("21", strings.TrimSpace(values[2]))
	suite.Equal("U", strings.TrimSpace(values[3]))
}

func (suite *QuantaTestSuite) TestXDML3Delete() {
	results, _, err := suite.runQuery("select date, name, age, gender from dmltest", nil)
	assert.Nil(suite.T(), err)
	suite.Equal(1, len(results))
	values := strings.Split(results[0], ",")
	upd := fmt.Sprintf("delete from dmltest where date = '%s' and name = '%s'", values[0], values[1])
	err, _ = suite.runDML(upd, nil)
	assert.Nil(suite.T(), err)
	qry := fmt.Sprintf("select count(*) from dmltest where date = '%s' and name = '%s'", values[0], values[1])
	results, _, err2 := suite.runQuery(qry, nil)
	assert.NoError(suite.T(), err2)
	suite.Equal("0", results[0])
}

// SELECT INTO AWS-S3
// Need to find a way to test S3 Integrations
//func (suite *QuantaTestSuite) TestCitiesSelectInfoS3tatement() {
//	results, err := suite.runQuery("select distinct prop_swid as swid, standard_ip as ip_address into 's3://guys-test/output.csv' from adobe_cricinfo where prop_swid != null and (standard_geo_country = 'ind' or evar_geo_country_code = 'ind' or prop_geo_country_code = 'ind') with delimiter = ',';")
//	assert.NoError(suite.T(), err)
//	assert.Greater(suite.T(), len(results), 0)
//	suite.Equal(1, len(results))
//}

// VIEWS - NOT YET IMPLEMENTED

// Create View
// func (suite *QuantaTestSuite) TestCreateView() {
// 	results, err := suite.runQuery("create view quanta_test_view as select * from cities")
// 	assert.NoError(suite.T(), err)
// 	assert.Greater(suite.T(), len(results), 0)
// 	suite.Equal(1, len(results))

// 	results, err = suite.runQuery("select * from quanta_test_view limit 10")
// 	assert.NoError(suite.T(), err)
// 	assert.Greater(suite.T(), len(results), 0)
// 	suite.Equal(10, len(results))
// }

// // Alter View
// func (suite *QuantaTestSuite) TestAlterView() {
// 	results, err := suite.runQuery("create view quanta_test_view as select state from cities")
// 	assert.NoError(suite.T(), err)
// 	assert.Greater(suite.T(), len(results), 0)
// 	suite.Equal(1, len(results))

// 	results, err = suite.runQuery("select * from quanta_test_view limit 10")
// 	assert.NoError(suite.T(), err)
// 	assert.Greater(suite.T(), len(results), 0)
// 	suite.Equal(10, len(results))
// }

// func (suite *QuantaTestSuite) TestDropView() {
// 	results, err := suite.runQuery("drop view quanta_test_view")
// 	assert.NoError(suite.T(), err)
// 	assert.Greater(suite.T(), len(results), 0)
// 	suite.Equal(1, len(results))

// 	results, err = suite.runQuery("select * from quanta_test_view limit 10")
// 	assert.NoError(suite.T(), err)
// 	assert.Equal(suite.T(), len(results), 0)
// 	suite.Equal(1, len(results))
// 	assert.EqualError(suite.T(), err, "View doesn't exist")
// }
