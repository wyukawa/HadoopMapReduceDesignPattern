package ch03;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.junit.Test;


public class StripseTest {

	@Test
	public void testRun() throws Exception {
		JobConf conf = new JobConf();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");

		Path input = new Path(System.getProperty("user.dir")
				+ "/src/test/resources/ch03/Stripse/input");
		Path output = new Path("output");

		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output);

		Stripse driver = new Stripse();
		driver.setConf(conf);

		int exitCode = driver.run(new String[] { input.toString(), output.toString() });
		assertThat(exitCode, is(0));

		checkOutput(conf, output);
	}

	private void checkOutput(JobConf conf, Path output) throws IOException {
		FileSystem fs = FileSystem.getLocal(conf);
		Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(output,
				new OutputLogFilter()));
		assertThat(outputFiles.length, is(1));

		BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
		BufferedReader expected = asBufferedReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("ch03/Stripse/expected.txt"));
		String expectedLine;
		while ((expectedLine = expected.readLine()) != null) {
			assertThat(actual.readLine(), is(expectedLine));
		}
		assertThat(actual.readLine(), nullValue());
		actual.close();
		expected.close();
	}

	private BufferedReader asBufferedReader(InputStream in) throws IOException {
		return new BufferedReader(new InputStreamReader(in));
	}

}
