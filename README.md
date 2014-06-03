Hadoop-Galaxy
=============

Light Hadoop-Galaxy integration.



Did you ever want to use Hadoop-based programs in your Galaxy workflows?  This
project can help you out with that.  Hadoop-Galaxy provides a light integration
between Hadoop-based tools and Galaxy, allowing you to run Hadoop tools from
Galaxy and mix them with regular tools in your workflows.


How to use it
-----------------

Install it via the tool shed.
.................................

Hadoop-Galaxy will add a new **pathset** data type to your Galaxy installation.
It will also install a few tools in your Galaxy menu which you might need to use in your
workflows.  Finally, it'll install a Python executable `hadoop_galaxy` that is
the adaptor you need to use to run Hadoop-based programs in Galaxy.


How to wrap your own Hadoop tool
..................................

You'll need to write a Galaxy wrapper for your tool.  For example, see the
wrapper for `dist_text_zipper`:


    <tool id="hg_dtz" name="Dist TextZipper">
      <description>Compress lots of text</description>
      <command>
        hadoop_galaxy
        --input $input
        --output $output
        --executable dist_text_zipper
      </command>
      <inputs>
        <param name="input" type="data" format="pathset"/>
      </inputs>
      <outputs>
        <data name="output" format="pathset" />
      </outputs>
      <stdio>
        <exit_code range="1:" level="fatal" />
      </stdio>
    </tool>

`dist_text_zipper` is a Hadoop program for compressing text data.  It is
bundled with Hadoop-Galaxy and the executable is found in the PATH.  It takes
two arguments:  one or more input paths and an output path.

To use it through Galaxy, we call `dist_text_zipper` through the
`hadoop_galaxy` adapter, specifying:

    * --input: input argument from Galaxy
    * --output: output argument from Galaxy
    * --executable: the name of the Hadoop program to call (in this case
      `dist_text_zipper`)

When writing your own wrapper for another Hadoop program, just replaced
`dist_text_zipper` with whatever the executable name is.  If your program
takes more arguments, just append them at the end of the command, after the
arguments you see in the example above; here is an example from
[seal-galaxy](https://github.com/crs4/seal-galaxy/blob/master/seal/bcl2qseq.xml>):

    hadoop_galaxy --input $input_data --output $output1 --executable seal bcl2qseq --ignore-missing-bcl
    
The adapter will take care of managing the pathset files provided by Galaxy and
will pass appropriate paths to the Hadoop program.


Pathsets
-------------

Here's an example pathset file:

    # Pathset       Version:0.0     DataType:Unknown
    hdfs://hdfs.crs4.int:9000/data/sample-cs3813-fastq-r1
    hdfs://hdfs.crs4.int:9000/data/sample-cs3813-fastq-r2

Pathsets are a list of paths.  Each path represents a part of the entire
dataset.  Directories include all files under their entire tree, in alphabetical
order.  You can also use the shell-like wildcard patters `?`, `*`, `[]`.
Order is important (the order of the parts determines the order of the
data in the overall dataset).


Workflows
--------------

TBA


Tools
-----------

TBA



Why is all this necessary
---------------------------

In most cases, Hadoop tools cannot be used directly from Galaxy for two reasons:

  * data are on HDFS, which isn't accessible to Galaxy
  * datasets are split in multiple files, which is Galaxy doesn't handle well
    (although they're working on it)

We've come with a solution that works by adding a layer of
indirection.  We created a new Galaxy datatype, the pathset, to which we write
the HDFS paths (or any other non-mounted filesystem, such as S3).  These are
serialized as text file that are handled handled directly by Galaxy as datasets.

Then, Hadoop-Galaxy provides an adapter program through which you call your Hadoop-based
program.  But, rather than passing the data paths directly to the adapter, you
pass it the pathset; the adapter takes care of reading the pathset files and
passing the real data paths to your program.



An important issue
-----------------------

An implication of the layer of indirection is that Galaxy knows nothing about
your actual data. Because of this, removing the Galaxy datasets does not delete
the files produced by your Hadoop runs, potentially resulting in the waste of a
lot of space.  In addition, as typical with pointers you can end up in
situations where multiple pathsets point to the same data, or where they point
to data that you want to access from Hadoop but would not want to delete (e.g.,
your run directories).

A proper solution would include a garbage collection system to be run with
Galaxy's clean up action, but we haven't implemented this yet.  Instead, at the
moment we handle this issue as follows.  Since we only use Hadoop for
intermediate steps in our pipeline, we don't permanently store any of its
output.  So, we write this data to a temporary storage space.  From time to
time, we stop the pipeline and remove the entire contents.


Authors
-------------

Luca Pireddu <pireddu@crs4.it>
Nicola Soranzo <soranzo@crs4.it>


