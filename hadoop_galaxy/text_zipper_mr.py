
import pydoop.hdfs as phdfs
import os

def mapper(_, line, writer, conf):
    chunk_size = 2 * 2**20 # 2 MB
    print "Using a chunk size of %s bytes (%0.1f KB)" % (chunk_size, float(chunk_size) / 2**10)

    input_dir, _, input_name = line.rstrip('\n').split('\t')
    full_input_path = os.path.join(input_dir, input_name)
    status_msg = "Compressing %s" % full_input_path
    print "processing file", full_input_path, "in mapper mapred.task.id", conf['mapred.task.id']

    writer.status(status_msg)
    last_notification = 0

    with phdfs.open( full_input_path ) as f:
        status_msg += " (%%0.1f / %0.1f MB)" % (float(f.size) / 2**20)
        writer.status(status_msg % 0)
        done = False

        def update_status(force=False):
            tell = f.tell()
            last = last_notification
            if force or tell - last > 10000000: # 10 MB
                msg = status_msg % (float(tell) / 2**20)
                writer.status(msg)
                print msg
                return tell
            else:
                return last

        while not done:
            writer.progress()
            chunk = f.read(chunk_size)
            if chunk:
                rest_of_line = f.readline()
                if rest_of_line:
                    chunk += rest_of_line.rstrip('\r\n')
                else:
                    chunk = chunk.rstrip('\r\n')
                    # although we know here that we're at the end of the file
                    # (rest_of_line was empty), we'll let the code exit only
                    # the 'else' below, forcing a status update as well.
                writer.emit('', chunk)
                last_notification = update_status()
            else:
                last_notification = update_status(True)
                done = True

# vim: expandtab tabstop=4 shiftwidth=4 autoindent
