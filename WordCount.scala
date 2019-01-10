Object WordCount{
	def main(args:Array[String]){
	//创建sparkconf的对象 并设置参数
		val conf = new SparkConfi().setAppName("WordCount").setMaster("local[2]")
	//创建sparkcontext对象
		val sc = new SparkContext(conf)
	//textFile传入数据的目录 返回一个RDD
		val lineRDD:RDD[String] = sc.textFile("d://AAA/b.txt")
	//使用RDD中的flatMap算子进行切分
		val wordRDD:RDD[String] = lineRDD.flatMap{
			line =>
			line.split(" ")
		}
	//使用Map算子遍历数据 把每个单词映射为(word,1)
		val pairRDD:RDD[(String,Int)] = wordRDD.map{
			word =>
			(word,1)
		}
	//根据key聚合
		val resRDD:RDD[(String,Int)]pairRDD.reduceByKey{
			case (v1,v2) =>
				  v1 + v2
		}
	//遍历去出每个元素的值
		resRDD.foreash{
		case (k,v) =>
		println(k + "/t" + v)
		}
	//关闭sparkcontext对象
		sc.stop
	}


}