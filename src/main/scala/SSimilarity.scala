package ssimilarity
import scala.collection.mutable /*{{{*/

import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Job
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

object Ssimilarity extends HadoopInterop {

  val LOG = Logger.getRootLogger()

  object FirstPhase {
    class ToUserPrefsMapper
    extends SMapper[LongWritable, Text, Text, Text] {
      override def map(key: LongWritable, line: Text, context: Context) {
        val arr = line.toString.split(",")
        context.write(arr.mkString(","))

        // if (arr.size < 3) {
        //   arr.foreach(node =>
        //     context.write(new Text(node), new Text(arr.mkString(","))))
        // } else {
        //   context.write(new Text(arr(1)), new Text(arr(0)+"\t"+FromZoneFile))
        // }
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
      job.setReducerClass(classOf[IdentityReducer])

      job.setInputFormatClass(classOf[TextInputFormat])
      FileInputFormat.setInputPaths(job, conf.get("ssimilarity.input"))
      job.setOutputFormatClass(classOf[SequenceFileOutputFormat[Text, Text]])

      FileOutputFormat.setOutputPath(job, conf.get("ssimilarity.itemvectorspath"))

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])

      return job
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

    // SSimilarity.LOG.info("Output directory: ")
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
