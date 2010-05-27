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

    conf.set("SSimilarity.input",
                   cl.getOptionValue("input", "inputfiles"))
    conf.set("SSimilarity.outputdir",
                   cl.getOptionValue("outputdir", "output"))

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
