package com.asuscomm.jkh120.code01.batch.configuration;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

import com.asuscomm.jkh120.code01.batch.domain.Game;
import com.asuscomm.jkh120.code01.batch.repository.GameRepository;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	// private static final String JSON_FILE_PATH = "C:/Users/Jeong/Desktop/";
	private static final String JSON_FILE_PATH = "/root/dev/java/batch/code01/";

	@Autowired
	JobBuilderFactory jobBuilerFactory;

	@Autowired
	StepBuilderFactory stepBuilderFactory;

	@Autowired
	private GameRepository gameRepository;

	@Autowired
	private SimpleJobLauncher jobLauncher;

	@Bean
	public SimpleJobLauncher jobLauncher(JobRepository jobRepository) {
		SimpleJobLauncher launcher = new SimpleJobLauncher();

		launcher.setJobRepository(jobRepository);

		return launcher;
	}

	// @Scheduled(fixedRate = 10000)
	@Scheduled(cron = "0 0 */2 * * *")
	public void runJob() throws Exception {
		System.out.println("Job Started at :" + new Date());

		JobParameters param = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
				.toJobParameters();

		JobExecution execution = jobLauncher.run(crawlerJob(), param);

		System.out.println("Job finished with status :" + execution.getStatus());
	}

	@Bean
	Job crawlerJob() {
		return jobBuilerFactory.get("crawlerJob").incrementer(new RunIdIncrementer()).start(crawlerStep()).build();
	}

	@Bean
	public StepExecutionListener stepExecutionListener() {
		return new StepExecutionListener() {

			@Override
			public void beforeStep(StepExecution stepExecution) {

			}

			@Override
			public ExitStatus afterStep(StepExecution stepExecution) {
				// TODO Auto-generated method stub
				return null;
			}

		};
	}

	@Bean
	Step crawlerStep() {
		return stepBuilderFactory.get("crawlerStep").listener(stepExecutionListener()).tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				// TODO Auto-generated method stub

				Calendar date = Calendar.getInstance();

				List<String> dateList = new ArrayList<String>();

				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

				Calendar c1 = Calendar.getInstance();

				int preDay = 1;
				for (int i = 0; i <= preDay; i++) {
					c1.add(Calendar.DATE, -i);
					String strToday = sdf.format(c1.getTime());
					dateList.add(strToday);
				}

				for (String sDate : dateList) {

					List<Game> gameList = gameRepository.findByGameDate(sDate);

					for (Game game : gameList) {
						Long gameId = game.getGameId();

						CloseableHttpClient httpClient = HttpClients.createDefault();
						HttpGet httpGet = new HttpGet(
								"http://sports.media.daum.net/proxy/planus/planus/api/schedule/baseball/kbo/result.json?game_id="
										+ gameId);
						CloseableHttpResponse httpResponse = httpClient.execute(httpGet);

						BufferedReader bufferReader = new BufferedReader(
								new InputStreamReader(httpResponse.getEntity().getContent()));

						String line;
						StringBuffer buffer = new StringBuffer();

						while ((line = bufferReader.readLine()) != null) {
							buffer.append(line);
						}

						bufferReader.close();
						httpClient.close();

						String path = JSON_FILE_PATH;
						String fileName = "result_" + gameId + ".json";
						FileWriter fileWriter = new FileWriter(path + fileName);
						fileWriter.write((buffer.toString().trim()));
						fileWriter.flush();
						fileWriter.close();
					}
				}

				return null;
			}

		}).build();
	}
}
