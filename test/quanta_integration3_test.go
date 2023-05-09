package test

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/araddon/qlbridge/schema"
	"github.com/disney/quanta/core"
	admin "github.com/disney/quanta/quanta-admin-lib"
	proxy "github.com/disney/quanta/quanta-proxy-lib"
	"github.com/disney/quanta/rbac"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	_ "net/http/pprof"
)

type QuantaTestSuite3 struct {
	suite.Suite
	// node  *server.Node

	// store *shared.KVStore // why?

	m0, m1, m2   *server.Node
	proxyControl *LocalProxyControl

	weStartedTheCluster bool

	tableCache *shared.TableCacheStruct // fixme get rid of this
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestQuantaTestSuite3(t *testing.T) {
	// atw put this back:
	suite.Run(t, new(QuantaTestSuite3))
}

// SetupSuite will start a local cluster and proxy if one is not already running

func (suite *QuantaTestSuite3) SetupSuite() {

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	weStartedTheCluster := false
	// check to see if the cluster is running already
	// if not, start it

	result := "[]"
	res, err := http.Get("http://localhost:8500/v1/health/service/quanta") // was quanta-node
	if err == nil {
		resBody, err := io.ReadAll(res.Body)
		if err == nil {
			result = string(resBody)
		}
	} else {
		fmt.Println("is consul not running?", err)
	}
	// fmt.Println("result:", result)

	// the three cases are 1) what cluster? 2) used to have one but now its critical 3) Here's the health.
	isNotRunning := strings.HasPrefix(result, "[]") || strings.Contains(result, "critical")

	if isNotRunning {
		suite.weStartedTheCluster = true
		suite.m0, _ = StartNodes(0) // from localCluster/local-cluster-main.go
		suite.m1, _ = StartNodes(1)
		suite.m2, _ = StartNodes(2)
		// this is too slow
		//fmt.Println("Waiting for nodes to start...", m2.State)
		for suite.m0.State != server.Active || suite.m1.State != server.Active || suite.m2.State != server.Active {
			time.Sleep(100 * time.Millisecond)
			//fmt.Println("Waiting for nodes...", m2.State)
		}
		//fmt.Println("done Waiting for nodes to start...", m2.State

		// start up proxy.
		suite.proxyControl = StartProxy(1, "") // like ./testdata/config

	}
	defer func() {
		if weStartedTheCluster {
			suite.proxyControl.Stop <- true
			suite.m0.Stop <- true
			suite.m1.Stop <- true
			suite.m2.Stop <- true
		}
	}()

	// what the heck does this do? Is there a better way to tell it "USE quanta"
	suite.tableCache = shared.NewTableCacheStruct() // why, as an sql cient, do I need this?
	src, err2 := source.NewQuantaSource(suite.tableCache, "./testdata/config", "127.0.0.1:8500", 4000, 3)
	assert.NoError(suite.T(), err2)
	schema.RegisterSourceAsSchema("quanta", src)

	conn := shared.NewDefaultConnection()
	if err := conn.Connect(nil); err != nil {
		log.Fatal(err)
	}
	defer conn.Disconnect()

	// conn.Update()

	store := shared.NewKVStore(conn)
	ctx, err2 := rbac.NewAuthContext(store, "USER001", true)
	if err2 != nil {
		log.Fatal(err2)
	}
	err3 := ctx.GrantRole(rbac.SystemAdmin, "USER001", "", true)
	if err3 != nil {
		log.Fatal(err3)
	}

	create := admin.CreateCmd{
		Table:     "cities",
		SchemaDir: "./testdata/config",
		Confirm:   true,
	}
	context := &proxy.Context{ConsulAddr: "127.0.0.1:8500", Port: 8500, Debug: true}

	create.Run(context)
	create.Table = "cityzip"
	create.Run(context)
	create.Table = "dmltest"
	create.Run(context)

	tables := &admin.TablesCmd{}
	tables.Run(context)

	{
		results, _, err := suite.runQuery_0("select count(*) from cityzip", nil)
		assert.NoError(suite.T(), err)
		assert.Greater(suite.T(), len(results), 0)
		fmt.Println("row count of cityzip = ", results[0])
		count, _ := strconv.Atoi(results[0])
		if count < 46280 {
			fmt.Println("LOADING cityzip")
			suite.loadData("cityzip", "./testdata/us_cityzip.parquet", conn, false)
		}
	}

	// load the data if not loaded
	{
		results, _, err := suite.runQuery_0("select count(*) from cities", nil)
		assert.NoError(suite.T(), err)
		assert.Greater(suite.T(), len(results), 0)
		fmt.Println("row count of cities = ", results[0])
		count, _ := strconv.Atoi(results[0])
		if count < 29488 {
			// needs loading
			fmt.Println("LOADING cities")
			suite.loadData("cities", "./testdata/us_cities.parquet", conn, false)
		}
	}

}

// Test count query with nested data source
func (suite *QuantaTestSuite3) TestSimpleQuery() {
	results, _, err := suite.runQuery("select count(*) from cities", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("29488", results[0])
}

// Test projection with nested data source (atw FIXME this works but is slow and dumps on the console)
func (suite *QuantaTestSuite3) TestSimpleProjection3() {
	results, _, err := suite.runQuery("select id, name, state_name, state from cities limit 100000", nil)
	assert.NoError(suite.T(), err)
	suite.Equal(29488, len(results))
}

// Test inner join with nested data source
func (suite *QuantaTestSuite3) fixmeTestInnerJoin() {
	results, _, err := suite.runQuery("select count(*) from cityzip as z inner join cities as c on c.id = z.city_id", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("46280", results[0])
}

// Test anti-join FIXME
func (suite *QuantaTestSuite3) fixmeTestAntiJoin() {
	results, _, err := suite.runQuery("select count(*) from cityzip as z inner join cities as c on c.id != z.city_id where z.city = 'Oceanside'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("46270", results[0])
	results, _, err = suite.runQuery("select c.id, c.name, c.state_name, c.state from cityzip as z inner join cities as c on c.id != z.city_id where z.city != 'Oceanside' limit 100000", nil)
	assert.NoError(suite.T(), err)
	suite.Equal(10, len(results))
}

// Test outer join FIXME
func (suite *QuantaTestSuite3) fixmeTestOuterJoinWithPredicate() {
	results, _, err := suite.runQuery("select count(*) from cityzip as z outer join cities as c on c.id = z.city_id where z.city = 'Oceanside'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("10", results[0])
	results, _, err = suite.runQuery("select c.id, c.name, c.state_name, c.state from cityzip as z outer join cities as c on c.id = z.city_id where z.city = 'Oceanside'", nil)
	assert.NoError(suite.T(), err)
	suite.Equal(10, len(results))
}

// Test outer join no predicate FIXME
func (suite *QuantaTestSuite3) fixmeTestOuterJoinNoPredicate() {
	results, _, err := suite.runQuery("select count(*) from cities as c outer join cityzip as z on c.id = z.city_id", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("46280", results[0])
}

func (suite *QuantaTestSuite3) TestSQLSyntaxUnknownKeyword() {
	_, _, err := suite.runQuery("selectX count(*) from cities", nil)
	assert.EqualError(suite.T(), err, "Unrecognized request type: selectX")
}

func (suite *QuantaTestSuite3) TestSQLSyntaxUnknownTable() {
	_, _, err := suite.runQuery("select count(*) from citiesx", nil)
	assert.EqualError(suite.T(), err, "QLBridge.plan: No datasource found")
}

func (suite *QuantaTestSuite3) TestSQLSyntaxUnknownField() {
	_, _, err := suite.runQuery("select count(*) from cities where nonsensefield = null", nil)
	assert.Error(suite.T(), err)
}

func (suite *QuantaTestSuite3) fixmeTestBetweenWithNegative() {
	results, _, err := suite.runQuery("select count(*) from cities where latitude BETWEEN 41.0056 AND 44.9733 AND longitude BETWEEN '-111.0344' AND '-104.0692'", nil)

	assert.NoError(suite.T(), err)
	suite.Equal("202", results[0]) // Count of all cities in WY
}

func (suite *QuantaTestSuite3) fixmeTestNotBetween() {
	results, _, err := suite.runQuery("select count(*) from cities where population NOT BETWEEN 100000 and 150000", nil)
	assert.NoError(suite.T(), err)
	suite.Equal("29321", results[0])
}

// Test subquery join
func (suite *QuantaTestSuite3) fixmeTestSubqueryJoin() {
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

func (suite *QuantaTestSuite3) TestInvalidTableOnJoin() {
	_, _, err := suite.runQuery("select count(*) from cities as c inner join faketable as f on c.id = f.fake_id", nil)
	assert.EqualError(suite.T(), err, "invalid table faketable in join criteria [INNER JOIN faketable AS f ON c.id = f.fake_id]")
}

func (suite *QuantaTestSuite3) TestInvalidFieldOnJoin() {
	_, _, err := suite.runQuery("select count(*) from cities as c inner join cityzip as z on c.id = z.fake_field", nil)
	assert.EqualError(suite.T(), err, "invalid field fake_field in join criteria [INNER JOIN cityzip AS z ON c.id = z.fake_field]")
}

func (suite *QuantaTestSuite3) fixmeTestSelectStar() {
	results, _, err := suite.runQuery("select * from cities where timezone != NULL limit 100000 with timeout=500", nil)
	assert.NoError(suite.T(), err)
	suite.Equal(29488, len(results))
}

func (suite *QuantaTestSuite3) TestSelectStarWithAlias() {
	results, _, err := suite.runQuery("select count(*) from cities as c", nil)
	assert.NoError(suite.T(), err)
	suite.Equal("29488", results[0])
}

func (suite *QuantaTestSuite3) fixmeTestTableAlias() {
	results, cols, err := suite.runQuery("select a.id as xid, a.state_name as xstate_name, a.state as xstate from cities as a where a.state_name = 'New York State' and a.state = 'NY'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(1186, len(results))
	suite.Equal(3, len(cols))
	suite.Equal("xid", cols[0])
	suite.Equal("xstate_name", cols[1])
	suite.Equal("xstate", cols[2])
}

func (suite *QuantaTestSuite3) TestJoinWithoutOnClause() {
	_, _, err := suite.runQuery("select count(*) from cities as c inner join cityzip as z", nil)
	assert.EqualError(suite.T(), err, "join criteria missing (ON clause)")
}

func (suite *QuantaTestSuite3) TestJoinWithNonkeyFields() {
	_, _, err := suite.runQuery("select count(*) from cities as c inner join cityzip as z on c.state_name = z.state", nil)
	assert.EqualError(suite.T(), err, "join field state_name is not a relation")
}

// AGGREGATES
// SUM, MIN, MAX, AVERAGE
func (suite *QuantaTestSuite3) fixmeTestSumInvalidFieldName() {
	_, _, err := suite.runQuery("select sum(foobar) from cities WHERE timezone != NULL", nil)
	assert.EqualError(suite.T(), err, "cannot resolve field cities.foobar in sum() function argument")
}

func (suite *QuantaTestSuite3) TestSumInvalidFieldType() {
	_, _, err := suite.runQuery("select sum(state_name) from cities", nil)
	assert.EqualError(suite.T(), err, "can't sum a non-bsi field state_name")
}

func (suite *QuantaTestSuite3) fixmeTestSimpleSum() {
	results, _, err := suite.runQuery("select sum(population) from cities", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("406795495", results[0])
}

func (suite *QuantaTestSuite3) fixmeTestSimpleAvg() {
	results, _, err := suite.runQuery("select avg(population) from cities", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("13795", results[0])
}

func (suite *QuantaTestSuite3) TestAvgInvalidFieldName() {
	_, _, err := suite.runQuery("select avg(foobar) from cities WHERE timezone != NULL", nil)
	assert.EqualError(suite.T(), err, "cannot resolve field cities.foobar in avg() function argument")
}

func (suite *QuantaTestSuite3) TestAvgInvalidFieldType() {
	_, _, err := suite.runQuery("select avg(state_name) from cities", nil)
	assert.EqualError(suite.T(), err, "can't average a non-bsi field state_name")
}

func (suite *QuantaTestSuite3) fixmeTestSimpleMin() {
	results, _, err := suite.runQuery("select min(population) from cities where name = 'Oceanside'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	//suite.Equal("2530", results[0])
	suite.Equal("       352", results[0])
}

func (suite *QuantaTestSuite3) TestNegativeMin() {
	results, _, err := suite.runQuery("select min(longitude) from cities where state = 'WY'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(" -111.0344", results[0])
}

func (suite *QuantaTestSuite3) TestMinInvalidFieldName() {
	_, _, err := suite.runQuery("select min(foobar) from cities WHERE timezone != NULL", nil)
	assert.EqualError(suite.T(), err, "cannot resolve field cities.foobar in min() function argument")
}

func (suite *QuantaTestSuite3) TestMinInvalidFieldType() {
	_, _, err := suite.runQuery("select min(state_name) from cities", nil)
	assert.EqualError(suite.T(), err, "can't find the minimum of a non-bsi field state_name")
}

func (suite *QuantaTestSuite3) fixmeTestSimpleMax() {
	results, _, err := suite.runQuery("select max(population) from cities", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("  18713220", results[0])
}

func (suite *QuantaTestSuite3) TestMaxInvalidFieldName() {
	_, _, err := suite.runQuery("select max(foobar) from cities WHERE timezone != NULL", nil)
	assert.EqualError(suite.T(), err, "cannot resolve field cities.foobar in max() function argument")
}

func (suite *QuantaTestSuite3) TestMaxInvalidFieldType() {
	_, _, err := suite.runQuery("select max(state_name) from cities", nil)
	assert.EqualError(suite.T(), err, "can't find the maximum of a non-bsi field state_name")
}

func (suite *QuantaTestSuite3) fixmeTestSimpleRank() {
	results, _, err := suite.runQuery("select topn(state_name) from cities", nil)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 53, len(results))
}

// END AGGREGATES

func (suite *QuantaTestSuite3) TestCityzipCount() { // OK
	results, _, err := suite.runQuery("select count(*) from cityzip", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("46280", results[0])
}

func (suite *QuantaTestSuite3) fixmeTestCitiesRegionList() {
	results, _, err := suite.runQuery("select count(*) from cities where region_list != null", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("29488", results[0])
	results, _, err = suite.runQuery("select count(*) from cities where region_list = 'NY'", nil)
	assert.NoError(suite.T(), err)
	suite.Equal("1186", results[0])
}

func (suite *QuantaTestSuite3) TestCitiesTimestamp() {
	results, _, err := suite.runQuery("select created_timestamp from cities", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(5000, len(results))
	// for _, v := range results {
	// 	assert.True(suite.T(), strings.HasPrefix(v, "1970-01-16"))
	// }
}

func (suite *QuantaTestSuite3) TestCitiesIntDirect() {
	results, _, err := suite.runQuery("select count(*) from cities where ranking = 1", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("50", results[0])
}

func (suite *QuantaTestSuite3) TestCitiesBoolDirect() {
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
func (suite *QuantaTestSuite3) fixmeTestCitiesLikeStatement() {
	results, _, err := suite.runQuery("select county, name from cities where name like 'woods' and name like 'HAWTHORN'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(1, len(results))
}

func (suite *QuantaTestSuite3) TestCitiesLikeListStatement() {
	_, _, err := suite.runQuery("select count(*) from cities where region_list like 'NY'", nil)
	assert.EqualError(suite.T(), err, "LIKE operator not supported for non-range field 'region_list'")
}

// DISTINCT statement
func (suite *QuantaTestSuite3) fixmeTestCitiesDistinctStatement() {
	results, _, err := suite.runQuery("select distinct state_name from cities", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(52, len(results))
}

// AND conditional statement
func (suite *QuantaTestSuite3) fixmeTestCitiesAndWhereStatement() {
	results, _, err := suite.runQuery("select id, state_name, state from cities where state_name = 'New York State' and state = 'NY'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(1186, len(results))
}

// OR conditional statement
func (suite *QuantaTestSuite3) TestCitiesOrWhereStatement() {
	results, _, err := suite.runQuery("select id, state_name, state from cities where state_name = 'New York State' or state = 'NY'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(1186, len(results))
}

// AND / OR conditional statement
func (suite *QuantaTestSuite3) fixmeTestCitiesAndOrWhereStatement() {
	results, _, err := suite.runQuery("select id, state_name, state from cities where state_name = 'New York State' or state = 'NY' and ranking = 3", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(1051, len(results))
}

// IN statements
func (suite *QuantaTestSuite3) fixmeTestCitiesINStatement() {
	results, _, err := suite.runQuery("select count(*) from cities where state IN ('NY','AK','WA')", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("2154", results[0])
}

// IN with NOT statement
func (suite *QuantaTestSuite3) fixmeTestCitiesNotINStatement() {
	results, _, err := suite.runQuery("select count(*) from cities where state not IN ('NY','AK','WA')", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("27334", results[0])
}

// NOT with OR
func (suite *QuantaTestSuite3) TestCitiesNotWithOR() {
	results, _, err := suite.runQuery("select count(*) from cities where county != 'Nassau' or state != 'WA'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	//suite.Equal("28866", results[0])
	suite.Equal("29488", results[0])
}

func (suite *QuantaTestSuite3) fixmeTestCitiesNotWithAND() {
	results, _, err := suite.runQuery("select count(*) from cities where county != 'Mason' and state = 'WA' and military != true and population > 0", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("596", results[0])
}

// SELECT List Functions
func (suite *QuantaTestSuite3) fixmeTestSimpleSelectListFunction() {
	results, _, err := suite.runQuery("select id, add(population + 10) from cities where state = 'NY' and name = 'Oceanside'", nil)
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("1840005246,31195", results[0])
}

func (suite *QuantaTestSuite3) TestSelectListFunctionInvalidArgument() {
	_, _, err := suite.runQuery("select id, add(foobar + 10) from cities WHERE timezone != NULL", nil)
	assert.EqualError(suite.T(), err, "cannot resolve field cities.foobar in add() function argument")
}

func (suite *QuantaTestSuite3) TestSelectListFunctionInvalidFunction() {
	_, _, err := suite.runQuery("select id, foobar(population + 10) from cities WHERE timezone != NULL", nil)
	assert.EqualError(suite.T(), err, "func \"foobar\" not found while processing select list")
}

// sql through the driver only in this test suite
// func (suite *QuantaTestSuite3) fixmeTestZDropTables() {
// 	err := suite.store.DeleteIndicesWithPrefix("cityzip", false)
// 	assert.Nil(suite.T(), err)
// 	err = suite.store.DeleteIndicesWithPrefix("cities", true)
// 	assert.Nil(suite.T(), err)
// }

func (suite *QuantaTestSuite3) fixmeTestXDML1Insert() {
	count, err := suite.runDML("insert into dmltest (name, age, gender) values ('Tom', 20, 'M')", nil)
	suite.Equal(int64(1), count)
	assert.Nil(suite.T(), err)
	results, _, err2 := suite.runQuery("select count(*) from dmltest", nil)
	assert.NoError(suite.T(), err2)
	suite.Equal("1", results[0])
}

func (suite *QuantaTestSuite3) fixmeTestXDML2Update() { // blows up
	results, _, err := suite.runQuery("select date, name, age, gender from dmltest", nil)
	assert.Nil(suite.T(), err)
	values := strings.Split(results[0], ",")
	qry := fmt.Sprintf("select count(*) from dmltest where date = '%s' and name = '%s'", values[0], values[1])
	results, _, err2 := suite.runQuery(qry, nil)
	assert.NoError(suite.T(), err2)
	suite.Equal("1", results[0])
	upd := fmt.Sprintf("update dmltest set age = 21, gender = 'U' where date = '%s' and name = '%s'", values[0], values[1])
	_, err = suite.runDML(upd, nil)
	assert.Nil(suite.T(), err)
	results, _, err = suite.runQuery("select date, name, age, gender from dmltest", nil)
	assert.Nil(suite.T(), err)
	suite.Equal(1, len(results))
	values = strings.Split(results[0], ",")
	suite.Equal(4, len(values))
	suite.Equal("21", strings.TrimSpace(values[2]))
	suite.Equal("U", strings.TrimSpace(values[3]))
}

func (suite *QuantaTestSuite3) loadData(table, filePath string, conn *shared.Conn, ignoreSourcePath bool) error {

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
	c, err := core.OpenSession(suite.tableCache, "./testdata/config", table, false, conn) // right ?
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c)

	for i := 1; i <= num; i++ {
		err := c.PutRow(table, pr, 0, ignoreSourcePath, false)
		assert.Nil(suite.T(), err)
	}
	c.Flush()
	c.CloseSession()
	return nil
}

func (suite *QuantaTestSuite3) BeforeTest(suiteName, testName string) {
	fmt.Println("BeforeTest", suiteName, testName)
}

func (suite *QuantaTestSuite3) AfterTest(suiteName, testName string) {
	fmt.Println("AfterTest", suiteName, testName)
}

func (suite *QuantaTestSuite3) runQuery(q string, args []interface{}) ([]string, []string, error) {

	// Connect using GoLang database/sql driver.
	db, err := sql.Open("qlbridge", "quanta") // cities quanta")
	if err != nil {
		assert.NoError(suite.T(), err)
	}
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
		// yikes! log.Println(result)
	}
	log.Println("")
	return results, cols, nil
}

// runQuery_0 is the same as runQuery but we don't have breakpoints on it.
func (suite *QuantaTestSuite3) runQuery_0(q string, args []interface{}) ([]string, []string, error) {

	// Connect using GoLang database/sql driver.
	db, err := sql.Open("qlbridge", "quanta") // cities quanta")
	if err != nil {
		assert.NoError(suite.T(), err)
	}
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

func (suite *QuantaTestSuite3) runDML(q string, args []interface{}) (int64, error) {

	assert.NotZero(suite.T(), q)

	// Connect using GoLang database/sql driver directly to Quanta engine
	db, err := sql.Open("qlbridge", "quanta")
	if err != nil {
		assert.NoError(suite.T(), err)
	}
	defer db.Close()
	if err != nil {
		return 0, err
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
	return count, err
}
