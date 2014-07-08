// org.apache.spark.rdd.RDD == Array

val pr = sc.textFile("hdfs://vbx:8020/user/yury/cfin/fwd_price_hr.tsv") // Array[String]
val vol = sc.textFile("hdfs://vbx:8020/user/yury/cfin/stg_site_volume_hr_51K.tsv") // Array[String]

val pr2 = pr.map(x => (x.split('\t')(2),x)) // Array[(String, String)]

val vol2 = vol.map(x => (x.split('\t')(1),x)) // Array[(String, String)]

val rdd2 = pr2.join(vol2) // Array[(String, (String, String))]

val rdd3 = rdd2.map{case(k,v) => (v._1 + "\t" + v._2)}

class JoinClass(val mth: String, val onoff: String, val mmdd: String, val p00: Double, val p01: Double, val p02: Double, val p03: Double, val p04: Double, val p05: Double, val p06: Double, val p07: Double, val p08: Double, val p09: Double, val p10: Double, val p11: Double, val p12: Double, val p13: Double, val p14: Double, val p15: Double, val p16: Double, val p17: Double, val p18: Double, val p19: Double, val p20: Double, val p21: Double, val p22: Double, val p23: Double, val dly: Double, val site_id: String, val mmdd2: String, val v00: Double, val v01: Double, val v02: Double, val v03: Double, val v04: Double, val v05: Double, val v06: Double, val v07: Double, val v08: Double, val v09: Double, val v10: Double, val v11: Double, val v12: Double, val v13: Double, val v14: Double, val v15: Double, val v16: Double, val v17: Double, val v18: Double, val v19: Double, val v20: Double, val v21: Double, val v22: Double, val v23: Double) {
  override def toString = s"$site_id $mth $onoff $mmdd $p00 $p01 $p02 $v00 $v01 $v02"
}

val rdd4 = rdd3.map(_.split('\t')).map(p => new JoinClass(p(0),p(1),p(2),p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble, p(9).toDouble, p(10).toDouble, p(11).toDouble, p(12).toDouble, p(13).toDouble, p(14).toDouble, p(15).toDouble, p(16).toDouble, p(17).toDouble, p(18).toDouble, p(19).toDouble, p(20).toDouble, p(21).toDouble, p(22).toDouble, p(23).toDouble, p(24).toDouble,p(25).toDouble, p(26).toDouble, p(27).toDouble, p(28),p(29),p(30).toDouble,p(31).toDouble, p(32).toDouble, p(33).toDouble, p(34).toDouble, p(35).toDouble, p(36).toDouble, p(37).toDouble, p(38).toDouble, p(39).toDouble, p(40).toDouble, p(41).toDouble, p(42).toDouble, p(43).toDouble, p(44).toDouble, p(45).toDouble, p(46).toDouble, p(47).toDouble, p(48).toDouble, p(49).toDouble, p(50).toDouble, p(51).toDouble, p(52).toDouble, p(53).toDouble))


val rdd5 = for(r <- rdd4) yield (r.mth, r.site_id, r.onoff, r.mmdd, r.dly, r.p00*r.v00+r.p01*r.v01+r.p02*r.v02+r.p03*r.v03+r.p04*r.v04+r.p05*r.v05+r.p06*r.v06+r.p07*r.v07+r.p08*r.v08+r.p09*r.v09+r.p10*r.v10+r.p11*r.v11+r.p12*r.v12+r.p13*r.v13+r.p14*r.v14+r.p15*r.v15+r.p16*r.v16+r.p17*r.v17+r.p18*r.v18+r.p19*r.v19+r.p20*r.v20+r.p21*r.v21+r.p22*r.v22+r.p23*r.v23) // Array[(String, String, String, String, Double, Double)]

// rdd5.foreach(println)

val rdd6 = rdd5.groupBy(x => (x._1, x._2, x._3)) map {
      case (k, v) =>
        (k._1, k._2, k._3, (v map (_._5) sum), (v map (_._6) sum))
}

// rdd6.foreach(println)

rdd6.saveAsTextFile("hdfs://vbx:8020/user/yury/cfout2")


