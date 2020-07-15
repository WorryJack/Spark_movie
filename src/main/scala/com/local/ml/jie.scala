package com.local.ml

import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object jie {
  val tokenCounts = tokenRdd.groupBy(line => line)
    .filter(grouped => grouped._2.size >= 5)
  val dictRdd = tokenCounts.map(line => line._1) dictRdd.saveAsTextFile("Mdict_file path")
  //把用户的评价记录转换成训练集
  val movieVectors = movieRdd.map(1 => { val strs = 1.split("1"）
    (strs(O), Vectors.dense((strs•slice(1, strs.size)).map(f => f.toDouble)))
  })
  val ratingWithVectors = ratingWithMovie.j oin(movieVectors)
  •flatMap(joined => {
    joined._2._1.map(item => (item.—2, j oined._2._2, item._3))
  })
  val ratingWithLabeled = ratingWithVectors.map(item => {
    (item._l, LabeledPoint(item.—3, item._2))
  })
  //利用朴素贝叶斯分类器进行训练和预测
  val usermodel = NaiveBayes. train (userTraing, lanibda = 1.0, "multinomialn")
  val prediction = usermodel.predict(usettopredict)

}
