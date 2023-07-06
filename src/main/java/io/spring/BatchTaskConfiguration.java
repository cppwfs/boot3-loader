/*
 * Copyright 2021-2022 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.spring;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.task.batch.listener.TaskBatchDao;
import org.springframework.cloud.task.batch.listener.support.JdbcTaskBatchDao;
import org.springframework.cloud.task.repository.TaskExecution;
import org.springframework.cloud.task.repository.TaskRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.*;

@Configuration
@EnableConfigurationProperties({ Boot3LoadProperties.class })
public class BatchTaskConfiguration {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private TaskRepository taskRepository;

	@Autowired
	private Boot3LoadProperties boot3LoadProperties;

	@Bean
	public CommandLineRunner commandLineRunner(DataSource dataSource, JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		return new CommandLineRunner() {
			Job job = new JobBuilder("LoadGenerated Batch 5.0", jobRepository )
					.start(new StepBuilder("job1step1", jobRepository)
							.<String, String>chunk(5, transactionManager)
							.reader(new ListItemReader<>(Collections.singletonList("hi")))
							.writer(new ListItemWriter<>())

							.build()).incrementer(new RunIdIncrementer())
							.build();
			@Override
			public void run(String... args) throws Exception {
				List<String> commandLineArgs = new ArrayList<>();
				commandLineArgs.add("-–spring.cloud.task.tablePrefix=BOOT3_TASK_");
				commandLineArgs.add("-–spring.batch.jdbc.table-prefix=BOOT3_BATCH_");

				for (int x = 0; x < boot3LoadProperties.jobsToCreate ; x++) {
					String uuid = UUID.randomUUID().toString();

					JobExecution jobExecution  = jobLauncher.run(job, new JobParameters(Collections.singletonMap("test", new JobParameter(uuid, String.class, true))));
					TaskExecution taskExecution = taskRepository.createTaskExecution();
					taskRepository.startTaskExecution(taskExecution.getExecutionId(), "Demo Batch Job Task 5.0", LocalDateTime.now(),
							commandLineArgs, null);
					TaskBatchDao taskBatchDao = new JdbcTaskBatchDao(dataSource, "BOOT3_TASK_");
					taskBatchDao.saveRelationship(taskExecution, jobExecution);
					taskRepository.completeTaskExecution(taskExecution.getExecutionId(), 0, LocalDateTime.now(),
							"COMPLETE");

				}
			}
		};
	}

}

