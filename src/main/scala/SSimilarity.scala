package ssimilarity
import scala.collection.mutable._ /*{{{*/

import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.log4j.Logger
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Options
import org.apache.commons.cli.{Option => CmdOption}
import org.apache.commons.cli.OptionBuilder/*}}}*/


object Main {
  def main(args: Array[String]) : Unit = {
    val result = ToolRunner.run(new Configuration(), new SSimilarity(), args);
    System.exit(result)
  }
}


object SSimilarity extends HadoopInterop {

  val LOG = Logger.getRootLogger()

  object FirstPhase {

    // outputs item_id mapped to user preferences
    class ToUserPrefsMapper
    extends SMapper[LongWritable, Text, Text, Text] {
      override def map(key: LongWritable, line: Text, context: Context) {
        val cols = line.toString.split(",")
        var (user_id, item_id, pref)= (cols(0), cols(1), cols(2))
        context.write(item_id, List(user_id, pref).mkString(","))
      }
    }

    // group each item_id with its list of user preferences   
    class ToItemVectorReducer extends SReducer[Text, Text, Text, Text] {
      override def reduce(key: Text, values: Iterable[Text], context:Context) {
        var prefs = values.foldLeft(new ListBuffer[String]()) { 
          (acc, value) => acc += value; acc
        }
        context.write(key, prefs.mkString("|"))
      }
    }
    
    def run(conf: Configuration) : Boolean = {
      LOG.info("Running FirstPhase.")
      val job = joinJob(conf)
      job.waitForCompletion(true)
    }

    def joinJob(conf: Configuration) : Job = {
      val job = new Job(conf, "create item vectors")
      job.setJarByClass(classOf[SSimilarity])
      job.setMapperClass(classOf[ToUserPrefsMapper])
      job.setReducerClass(classOf[ToItemVectorReducer])

      job.setInputFormatClass(classOf[TextInputFormat])
      FileInputFormat.setInputPaths(job, conf.get("ssimilarity.input"))
      job.setOutputFormatClass(classOf[SequenceFileOutputFormat[Text, Text]])
      // job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

      FileOutputFormat.setOutputPath(job, conf.get("ssimilarity.itemvectorspath"))

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])

      return job
    }
  }

  object SecondPhase {
    implicit def string2parsedTuple(string: String) = new {
      def parsedItemPref() : Tuple2[String, Double] = {
          var cols = string.split(",") 
          return (cols(0), java.lang.Double.parseDouble(cols(1)))
      }
    }

    // for each item-vector, we compute its length here and map out all entries with the user as key,
    // so we can create the user-vectors in the reducer
    class PreferredItemsPerUserMapper extends SMapper[Text, Text, Text, Text] {
      override def map(itemId: Text, userPrefs: Text, context: Context) {
        var length = 0.0D

        userPrefs.split("\\|").foreach(pref => {
          var (userId, prefVal) = pref.parsedItemPref
          length += prefVal * prefVal 
        })
  
        length = java.lang.Math.sqrt(length)

        userPrefs.split("\\|").foreach(pref => { // todo, combine with above
          var (userId, prefVal) = pref.parsedItemPref 
          context.write(userId, List(itemId, length, prefVal).mkString(","))
        })
      }
    }

    class PreferredItemsPerUserReducer extends SReducer[Text, Text, Text, Text] {
      override def reduce(userId: Text, values: Iterable[Text], context:Context) {
        var prefs = values.foldLeft(new ListBuffer[String]()) { (acc, value) => acc += value; acc }
        context.write(userId, prefs.mkString("|"))
      }
    }
    
    def run(conf: Configuration) : Boolean = {
      LOG.info("Running SecondPhase.")
      val job = joinJob(conf)
      job.waitForCompletion(true)
    }

    def joinJob(conf: Configuration) : Job = {
      val job = new Job(conf, "create user vectors")
      job.setJarByClass(classOf[SSimilarity])
      job.setMapperClass(classOf[PreferredItemsPerUserMapper])
      job.setReducerClass(classOf[PreferredItemsPerUserReducer])

      // job.setInputFormatClass(classOf[TextInputFormat])
      job.setInputFormatClass(classOf[SequenceFileInputFormat[Text, Text]])
      FileInputFormat.setInputPaths(job, conf.get("ssimilarity.itemvectorspath"))
      // job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
      job.setOutputFormatClass(classOf[SequenceFileOutputFormat[Text, Text]])
      FileOutputFormat.setOutputPath(job, conf.get("ssimilarity.uservectorspath"))

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])

      return job
    }
  }

  object ThirdPhase {
    implicit def string2parsedTuple(string: String) = new {
      def parsedItemPref() : Tuple3[String, Double, Double] = {
          var cols = string.split(",") 
          return (cols(0), java.lang.Double.parseDouble(cols(1)), java.lang.Double.parseDouble(cols(2)))
      }
    }

    // map out each pair of items that appears in the same user-vector together with the multiplied vector lengths
    // of the associated item vectors
    class CopreferredItemsMapper extends SMapper[Text, Text, Text, Text] {
      override def map(userId: Text, itemPrefLines: Text, context: Context) {
        var itemPrefs = itemPrefLines.split("\\|")
        
        itemPrefs.zipWithIndex.foreach { case (pref, n) => { // todo, combine with above
          var (itemNId, itemNLength, itemNPrefVal) = pref.parsedItemPref

          var m = n + 1
          while(m < itemPrefs.length) { 
            var (itemMId, itemMLength, itemMPrefVal) = itemPrefs(m).parsedItemPref

            val idsSorted = List(itemNId, itemMId).sort((e1, e2) => (e1 compareTo e2) < 0)
            val itemAId = idsSorted.first
            val itemBId = idsSorted.last

            val pair = List(itemAId, itemBId, itemNLength * itemMLength).mkString(",")

            context.write(pair, (itemNPrefVal * itemMPrefVal).toString)
            m += 1
          }

        }}
      }
    }

    class CosineSimilarityReducer extends SReducer[Text, Text, Text, Text] {
      override def reduce(userId: Text, values: Iterable[Text], context:Context) {
        // var prefs = values.foldLeft(new ListBuffer[String]()) { (acc, value) => acc += value; acc }
        // context.write(userId, prefs.mkString("|"))
        for (value <- values) {
          context.write(userId, value)
        }
      }
    }
    
    def run(conf: Configuration) : Boolean = {
      LOG.info("Running ThirdPhase.")
      val job = joinJob(conf)
      job.waitForCompletion(true)
    }

    def joinJob(conf: Configuration) : Job = {
      val job = new Job(conf, "create item similarity")
      job.setJarByClass(classOf[SSimilarity])
      job.setMapperClass(classOf[CopreferredItemsMapper ])
      job.setReducerClass(classOf[CosineSimilarityReducer])

      // job.setInputFormatClass(classOf[TextInputFormat])
      job.setInputFormatClass(classOf[SequenceFileInputFormat[Text, Text]])
      FileInputFormat.setInputPaths(job, conf.get("ssimilarity.uservectorspath"))
      job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
      FileOutputFormat.setOutputPath(job, conf.get("ssimilarity.outputdir"))

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])

      return job
    }
  }


}



class SSimilarity extends Configured with Tool with HadoopInterop {
  def run(args: Array[String]): Int = {
    val conf = getRealConf(args)
    runPhases(conf)
  }

  def runPhases(conf: Configuration) : Int = {
    if (!SSimilarity.FirstPhase.run(conf)) return 1
    SSimilarity.LOG.info("Finished FirstPhase.")

    if (!SSimilarity.SecondPhase.run(conf)) return 1
    SSimilarity.LOG.info("Finished SecondPhase.")

    if (!SSimilarity.ThirdPhase.run(conf)) return 1 
    SSimilarity.LOG.info("Finished ThirdPhase.")

    SSimilarity.LOG.info("Output directory: " + conf.get("ssimilarity.outputdir"))
    return 0
  }

  def getRealConf(args: Array[String]) : Configuration = {
    val gp = new GenericOptionsParser(getConf(), additionalOptions(), args)
    val conf = gp.getConfiguration()

    val cl = gp.getCommandLine()
    if (cl == null) {
      System.exit(1)
    }

    conf.set("ssimilarity.input",
                   cl.getOptionValue("input", "inputfiles"))
    conf.set("ssimilarity.tmpdir",
                   cl.getOptionValue("tmpdir", "tmp"))
    conf.set("ssimilarity.outputdir",
                   cl.getOptionValue("outputdir", "output"))

    // setup our tmp paths
    conf.set("ssimilarity.itemvectorspath",
                   cl.getOptionValue("itemvectorspath", conf.get("ssimilarity.tmpdir") + "/itemVectors"))
    conf.set("ssimilarity.uservectorspath",
                   cl.getOptionValue("uservectorspath", conf.get("ssimilarity.tmpdir") + "/userVectors"))

    conf
  }

  def additionalOptions() : Options = {
    var input = new CmdOption("input",
                              true,
                              "Directory to read the user-item-matrix from")
    input.setType("")
    var output = new CmdOption("outputdir",
                               true,
                               "Directory to write all output to")
    input.setType("")
    val ops = new Options()
    ops.addOption(input)
    ops.addOption(output)
    ops
  }
}
