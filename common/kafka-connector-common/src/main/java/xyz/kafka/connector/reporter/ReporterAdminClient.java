package xyz.kafka.connector.reporter;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * ReporterAdminClient
 *
 * @author chaoxin.lu
 * @version V 1.0
 * @since 2022-04-20
 */
public class ReporterAdminClient {
    private final Map<String, Object> adminConfig;
    private static final Logger log = LoggerFactory.getLogger(ReporterAdminClient.class);

    public ReporterAdminClient(Map<String, Object> adminConfig) {
        this.adminConfig = adminConfig;
    }

    AdminClient initAdminClient(Map<String, Object> adminConfig) {
        return AdminClient.create(adminConfig);
    }

    public boolean createTopic(NewTopic newTopic) throws ConnectException {
        String topicName = newTopic.name();
        boolean created = false;
        try (AdminClient cli = this.initAdminClient(this.adminConfig)) {
            try {
                cli.createTopics(Collections.singleton(newTopic)).all().get();
                log.info("Created topic {} for reporter.", topicName);
                created = true;
            } catch (ExecutionException var18) {
                this.handleExecutionException(var18, topicName);
            } catch (InterruptedException var19) {
                Thread.currentThread().interrupt();
                throw new RetriableException("Could not create reporter topic " + topicName, var19);
            }
            try {
                TopicDescription[] topicDescriptions = cli.describeTopics(Collections.singleton(topicName))
                        .allTopicNames().get().values()
                        .toArray(TopicDescription[]::new);
                TopicDescription topicDescription = topicDescriptions[0];
                log.info("Using topic {}", topicDescription);
            } catch (InterruptedException | ExecutionException var17) {
                Thread.currentThread().interrupt();
                log.warn("Unable to retrieve information for topic {}:", topicName, var17);
            }
        }
        return created;
    }

    protected void handleExecutionException(ExecutionException e, String topicName) throws ConnectException {
        Throwable cause = e.getCause();
        if (cause instanceof TopicExistsException) {
            log.info("Found existing topic");
        } else if (cause instanceof UnsupportedVersionException) {
            log.warn("The target cluster does not support the CreateTopics API, so falling back to broker auto topic creation of topic {}",
                    topicName, e);
        } else if (cause instanceof ClusterAuthorizationException) {
            log.warn("Not authorized to access cluster, so falling back to broker auto topic creation of topic {}", topicName, e);
        } else {
            if (!(cause instanceof TopicAuthorizationException)) {
                if (cause instanceof TimeoutException) {
                    throw new RetriableException("Timeout exceeded when managing topics:", e);
                }
                throw new ConnectException("Unable to manage topics:", e);
            }
            log.warn("Not authorized to create topics,so falling back to broker auto topic creation of topic {}", topicName, e);
        }
    }
}
