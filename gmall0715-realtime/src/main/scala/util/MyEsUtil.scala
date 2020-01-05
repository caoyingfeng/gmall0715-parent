package util

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}
import org.apache.commons.beanutils.BeanUtils

/**
  * @author cy
  * @create 2019-12-18 17:54
  */
object MyEsUtil {
  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory:JestClientFactory = null  //工厂类，类似于连接池

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    //if (!Objects.isNull(client)) try
    if(client != null) try
      client.shutdownClient()
    //client.close()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def executeIndexBulk(indexName:String ,list:List[Any], idColumn:String): Unit ={
    //Bulk用来批处理，一批数据默认的index，默认的type
    val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
    for ( doc <- list ) {

      val indexBuilder = new Index.Builder(doc)
      //生成id
      if(idColumn!=null){
        val id: String = BeanUtils.getProperty(doc,idColumn)
        indexBuilder.id(id)
      }
      val index: Index = indexBuilder.build()
      bulkBuilder.addAction(index)
    }
    val jestclient: JestClient =  getClient

    val result: BulkResult = jestclient.execute(bulkBuilder.build())
    if(result.isSucceeded){
      println("保存成功:"+result.getItems.size())
    }
    close(jestclient)
  }
  //批次增加到es
  def insertEsBulk(indexName:String,sourceList:List[(String,Any)]): Unit ={
    if(sourceList.size>0) {
      val jest: JestClient = getClient
      //Bulk用来批处理
      val bulkBuilder: Bulk.Builder = new Bulk.Builder
      for ((id, source) <- sourceList) {
        val index: Index = new Index.Builder(source).index(indexName).`type`("_doc").id(id).build()
        bulkBuilder.addAction(index)
      }
      //批次提交到es  getItems 得到执行结果
      val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBuilder.build()).getItems
      println("已保存：" + items.size() + "条数据")

      close(jest)
    }
  }

  def main(args: Array[String]): Unit = {
    val jest: JestClient = getClient
    val index = new Index.Builder(new Stu0715("101","zhang3")).index("stu_0715").`type`("stu").id("101").build()
    //Action  中的index相当于add或insert
    jest.execute(index)
    jest.close()
  }

  case class Stu0715(stuId: String, name: String)

}
