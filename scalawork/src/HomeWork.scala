import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HomeWork {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("spack")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")
    val dataFrameReader = sparkSession.read.format("csv").option("header", "true").option("sep", " ").option("multiLine", true).option("encoding", "utf-8").load("file:///I:\\jkHomeWork\\scalawork\\data.csv")

    dataFrameReader.show
    dataFrameReader.createOrReplaceTempView("student")
    //  有多少个小于20岁的人参加考试; @author ly;
    print("有多少个小于20岁的人参加考试")
    sparkSession.sql("select count(distinct class) coun from student where age < 20").show
    // 3. 一共有多少个等于20岁的人参加考试？
    print("一共有多少个等于20岁的人参加考试？")
    sparkSession.sql("select count(distinct class) from student where age = 20").show
    // 4. 一共有多少个大于20岁的人参加考试？
    print("一共有多少个大于20岁的人参加考试？")
    sparkSession.sql("select count(distinct class) from student where age > 20").show
    // 5. 一共有多个男生参加考试？
    print("一共有多个男生参加考试？")
    sparkSession.sql("select count(distinct name) from student where sex = '男'").show
    // 6. 一共有多少个女生参加考试？
    print("一共有多少个女生参加考试？")
    sparkSession.sql("select count(distinct name) from student where sex = '女'").show
    // 7. 12班有多少人参加考试？
    print("12班有多少人参加考试？")
    sparkSession.sql("select count(distinct name) from student where class = 12").show
    // 8. 13班有多少人参加考试？
    println("13班有多少人参加考试？")
    sparkSession.sql("select count(distinct name) from student where class = 13").show
    // 9. 语文科目的平均成绩是多少？
    println("语文科目的平均成绩是多少？")
    sparkSession.sql("select avg(score) from student where suject='chinese' group by suject").show
    // 10. 数学科目的平均成绩是多少？
    println("数学科目的平均成绩是多少？")
    sparkSession.sql("select avg(score) from student where suject='math' group by suject").show
    // 11. 英语科目的平均成绩是多少？
    println("英语科目的平均成绩是多少？")
    sparkSession.sql("select avg(score) from student where suject='english' group by suject").show
    // 12. 每个人平均成绩是多少？
    println("每个人平均成绩是多少？")
    sparkSession.sql("select avg(score), name from student where group by name").show
    // 13. 12班平均成绩是多少？
    println("12班平均成绩是多少？")
    sparkSession.sql("select avg(score) from student where class=12 group by class").show
    // 14. 12班男生平均总成绩是多少？
    println("12班男生平均总成绩是多少？")
    sparkSession.sql("select avg(score) from student where class=12 and sex='男' group by class").show
    // 15. 12班女生平均总成绩是多少？
    println("12班女生平均总成绩是多少？")
    sparkSession.sql("select avg(score) from student where class=12 and sex='女' group by class").show
    // 16. 13班平均成绩是多少？
    println("13班平均成绩是多少？")
    sparkSession.sql("select avg(score) from student where class=12 group by class").show
    // 17. 13班男生平均总成绩是多少？
    println("13班男生平均总成绩是多少？")
    sparkSession.sql("select avg(score) from student where class=13 and sex='男' group by class").show
    // 18. 13班女生平均总成绩是多少？
    println("13班女生平均总成绩是多少？")
    sparkSession.sql("select avg(score) from student where class=13 and sex='女' group by class").show
    // 19. 全校语文成绩最高分是多少？
    println("全校语文成绩最高分是多少？")
    sparkSession.sql("select max(score) from student where suject='chinese' group by suject").show
    // 20. 12班语文成绩最低分是多少？
    println("12班语文成绩最低分是多少？")
    sparkSession.sql("select min(score) from student where suject='chinese' and class=12 group by class").show
    // 21. 13班数学最高成绩是多少？
    println("13班数学最高成绩是多少？")
    sparkSession.sql("select max(score) from student where suject='math' and class=13 group by class").show
    // 22. 总成绩大于150分的12班的女生有几个？
    println("总成绩大于150分的12班的女生有几个？")
    sparkSession.sql("select count(distinct name) from student where class=12 and sex='女' group by class having sum(score) > 150.0 ").show
    // 23. 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
    println("总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？")
    sparkSession.sql("select avg(s1.score), t1.name from (select t.name name from (select name from student where age >= 19 group by name having sum(score) > 150.0) t left join student s on t.name = s.name where s.suject='math' and s.score >= 70 ) t1 left join student s1 on t1.name = s1.name group by t1.name ").show
    sparkSession.stop()
  }

}

case class Student(classname: Int, name: String, age: Int, sex: String, suject: String, socre: Int)
