import com.tsurugidb.iceaxe.TsurugiConnector
import com.tsurugidb.iceaxe.metadata.TgTableMetadata
import com.tsurugidb.iceaxe.session.TsurugiSession
import com.tsurugidb.iceaxe.sql.{TsurugiSqlStatement, TsurugiSqlQuery}
import com.tsurugidb.iceaxe.sql.result.{
  TsurugiResultEntity,
  TsurugiStatementResult,
  TsurugiQueryResult
}
import com.tsurugidb.iceaxe.transaction.manager.{
  TgTmSetting,
  TsurugiTransactionManager
}
import com.tsurugidb.iceaxe.transaction.option.TgTxOption
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction

import com.tsurugidb.tsubakuro.common.{Session, SessionBuilder}
import com.tsurugidb.tsubakuro.sql.{SqlClient, Transaction}
import com.tsurugidb.tsubakuro.kvs.{KvsClient, RecordBuffer, TransactionHandle}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext}
import scala.jdk.CollectionConverters._
import scala.util.{Using, Try, Success, Failure}
import java.net.URI
// private val Connect ="tcp://localhost:12345"
private val Connect = "ipc://tsurugi"
private val TableName = "test_table"
private val ColumnList = List(1_000, 2_000, 3_000)
class Setting(val tg: TgTmSetting, val name: String) {
  def getName: String = name
  def getTgTmSetting: TgTmSetting = tg
}

class Table(
    val tableName: String,
    val format: String,
    val rowCount: Int,
    val columnCount: Int
) {
  def getTableName: String = tableName
  def getFormat: String = format
  def getRowCount: Int = rowCount
  def getColumnCount: Int = columnCount

  def createRecordBuffer(id: Int): RecordBuffer = {
    val record = new RecordBuffer()
    record.add("id", id)
    record.add("name", 1)
    record.add("note", 1)
    record
  }
}
def insert(kvs: KvsClient, table: Table)(implicit
    ec: ExecutionContext
): Unit = {
  println(s"insert ${table.getTableName} column ${table.getColumnCount}")
  Try {
    val tx = kvs.beginTransaction().await
    (0 until table.getColumnCount).foreach { i =>
      val record = table.createRecordBuffer(i)
      kvs.put(tx, table.getTableName, record).await
    }
    kvs.commit(tx).await
    tx.close()
  } recover { case e: Exception =>
    println(e.getMessage)
  }
}

def dropCreate(sql: SqlClient, t: Table)(implicit
    ec: ExecutionContext
): Unit = {
  val drop = s"DROP TABLE ${t.getTableName}"
  val create = s"CREATE TABLE ${t.getTableName} ${t.getFormat}"

  println(s"${drop}")
  Try {
    val transaction = sql.createTransaction().await
    transaction.executeStatement(drop).await
    transaction.commit().await
    transaction.close()
  } recover { case e: Exception =>
    println(e.getMessage)
  }

  println(s"${create}")
  Try {
    val transaction = sql.createTransaction().await
    transaction.executeStatement(create).await
    transaction.commit().await
    transaction.close()
  } recover { case e: Exception =>
    println(e.getMessage)
  }
}

def sqlExecute(
    session: Session,
    sql: SqlClient,
    kvs: KvsClient,
    table: Table
): Unit = {
  val createAndInsertTime = System.nanoTime()
  dropCreate(sql, table)
  insert(kvs, table)
  val createAndInsertEndTime = System.nanoTime()
  println(
    s"createAndInsert ${(createAndInsertEndTime - createAndInsertTime) / 1_000_000} ms"
  )
}

def executeSelect(session: TsurugiSession, setting: Setting): Unit = {
  println(setting.getName)
  var list = s"select max(name) from $TableName" ::
    s"select min(name) from $TableName" ::
    s"select avg(name) from $TableName" ::
    s"select sum(name) from $TableName" ::
    s"select count(name) from $TableName" ::
    s"select distinct name, note from $TableName" :: Nil

  val tm = session.createTransactionManager(setting.getTgTmSetting)
  list.foreach { sql =>
    using(session.createQuery(sql)) { ps =>
      println(sql)
      val start = System.nanoTime()
      val list = tm.execute((transaction: TsurugiTransaction) =>
        using(transaction.executeQuery(ps)) { result =>
          result.getRecordList.asScala.toList
        }
      ): List[TsurugiResultEntity]
      println(list)
      val end = System.nanoTime()
      println(s"${(end - start) / 1_000_000} ms")
    }
  }

}

def using[T <: AutoCloseable, R](resource: T)(f: T => R): R =
  try f(resource)
  finally resource.close()

@main def run(): Unit = {
  println("start")
  val endpoint = URI.create(Connect)
  val connector = TsurugiConnector.of(endpoint)
  ColumnList.foreach { columncount =>
    val table = new Table(
      TableName,
      "(id int primary key, name int, note int)",
      3,
      columncount
    )
    Using.Manager { use =>
      implicit val ec: ExecutionContext = ExecutionContext.global
      val session = use(SessionBuilder.connect(endpoint).create())
      val sql = use(SqlClient.attach(session))
      val kvs = use(KvsClient.attach(session))

      sqlExecute(session, sql, kvs, table)
    } match {
      case Success(_)         =>
      case Failure(exception) => println(s"error : ${exception.getMessage}")
    }
    val list =
      new Setting(TgTmSetting.ofAlways(TgTxOption.ofRTX()), "RTX") ::
        new Setting(TgTmSetting.ofAlways(TgTxOption.ofOCC()), "OCC") ::
        new Setting(TgTmSetting.ofAlways(TgTxOption.ofLTX()), "LTX") :: Nil
    using(connector.createSession()) { session =>
      list.foreach { setting =>
        executeSelect(session, setting)
      }
    }
  }
}
