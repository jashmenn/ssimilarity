package ssimilarity

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

import org.apache.hadoop.fs.Path

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BooleanWritable, IntWritable, LongWritable, FloatWritable, DoubleWritable, Text, UTF8}
import java.lang.{Iterable => JavaItb}
import java.util.{Iterator => JavaItr}
import java.io.File

trait HadoopInterop {
  implicit def conf2ComponentConf(conf: Configuration) = new {
    // extend JobConf here, if you'd like
  }

  implicit def writable2boolean(value: BooleanWritable) = value.get
  implicit def boolean2writable(value: Boolean) = new BooleanWritable(value)

  implicit def writable2int(value: IntWritable) = value.get
  implicit def int2writable(value: Int) = new IntWritable(value)

  implicit def writable2long(value: LongWritable) = value.get
  implicit def long2writable(value: Long) = new LongWritable(value)

  implicit def writable2float(value: FloatWritable) = value.get
  implicit def float2writable(value: Float) = new FloatWritable(value)

  implicit def writable2double(value: DoubleWritable) = value.get
  implicit def double2writable(value: Double) = new DoubleWritable(value)

  implicit def text2string(value: Text) = value.toString
  implicit def string2text(value: String) = new Text(value)

  implicit def uft82string(value: UTF8) = value.toString
  implicit def string2utf8(value: String) = new UTF8(value)

  implicit def path2string(value: Path) = value.toString
  implicit def string2path(value: String) = new Path(value)


  class UnjackedIterable[T](private val jtb: JavaItb[T]) extends Iterable[T] {
    def elements: Iterator[T] = jtb.iterator
  }
  
  class UnjackedIterator[T](private val jit: JavaItr[T]) extends Iterator[T] {
    def hasNext: Boolean = jit.hasNext
    
    def next: T = jit.next
  }

  implicit def jitb2sitb[T](jtb: JavaItb[T]): Iterable[T] = new UnjackedIterable(jtb)
  implicit def jitr2sitr[T](jit: JavaItr[T]): Iterator[T] = new UnjackedIterator(jit)

  class SMapper[A,B,C,D] extends Mapper[A,B,C,D] {
    type Context = Mapper[A,B,C,D]#Context
  }

  class SReducer[A,B,C,D] extends Reducer[A,B,C,D] {
    type Context = Reducer[A,B,C,D]#Context

    override def reduce(key: A, values: JavaItb[B], context: Context) {
      reduce(key, jitb2sitb(values), context)
    }

    // This prettys up our code by letting us use a real iterable
    // instead of Java's terrible one.
    def reduce(key: A, values: Iterable[B], context: Context) {
      for (value <- values) {
        context.write(key.asInstanceOf[C], value.asInstanceOf[D])
      }
    }
  }

  // Because hadoop requires it and type erasure is awful.
  class TextArrayWritable(klass: java.lang.Class[_ <: Writable])
                                           extends ArrayWritable(klass) {
    // This should really use UTF8, but we're already on this train,
    // so let's ride it.
    def this() = this(classOf[Text])
    def this(strings: Array[String]) = {
      this(classOf[Text])
      set(strings.map(new Text(_)))
    }
    def this(texts: Array[Text]) = {
      this(classOf[Text])
      set(texts)
    }

    def set(texts: Array[Text]) : Unit = {
      set(texts.asInstanceOf[Array[Writable]])
    }

    // this is for people who like to debug with TextOutputFormat
    override def toString() : String = toStrings.mkString(",")

  }

  // For Iterable.min(Iterable[Text])
  class RichText(protected val txt: Text) extends Ordered[Text] {
    def compare(that: Text) = txt.compareTo(that)
    def compare(that: RichText) = txt.compareTo(that.txt)
  }

  implicit def text2RichText(txt: Text) : RichText = new RichText(txt)
}
