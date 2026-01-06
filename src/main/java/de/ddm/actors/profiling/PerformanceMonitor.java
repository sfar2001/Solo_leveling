package de.ddm.actors.profiling;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

public class PerformanceMonitor extends AbstractBehavior<PerformanceMonitor.Message> {

    public interface Message extends AkkaSerializable {}

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TaskStartMessage implements Message {
        private static final long serialVersionUID = 1L;
        String taskId;
        String workerName;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TaskEndMessage implements Message {
        private static final long serialVersionUID = 2L;
        String taskId;
        String workerName;
        int indsFound;
        long durationMs;
    }

    @NoArgsConstructor
    public static class StatusRequestMessage implements Message {
        private static final long serialVersionUID = 3L;
    }

    public static final String DEFAULT_NAME = "performanceMonitor";

    public static Behavior<Message> create() {
        return Behaviors.setup(PerformanceMonitor::new);
    }

    private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private long startTime = System.currentTimeMillis();
    private int tasksCompleted = 0;
    private int totalINDs = 0;
    private long totalProcessingTime = 0;

    private PerformanceMonitor(ActorContext<Message> context) {
        super(context);
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(TaskStartMessage.class, this::handle)
                .onMessage(TaskEndMessage.class, this::handle)
                .onMessage(StatusRequestMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(TaskStartMessage message) {
        getContext().getLog().debug("Task started: {} on {}",
                message.getTaskId(), message.getWorkerName());
        return this;
    }

    private Behavior<Message> handle(TaskEndMessage message) {
        tasksCompleted++;
        totalINDs += message.getIndsFound();
        totalProcessingTime += message.getDurationMs();

        getContext().getLog().debug("Task completed: {} found {} INDs in {}ms",
                message.getTaskId(), message.getIndsFound(), message.getDurationMs());

        return this;
    }

    private Behavior<Message> handle(StatusRequestMessage message) {
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        long uptime = System.currentTimeMillis() - startTime;

        getContext().getLog().info("=== PERFORMANCE STATUS ===");
        getContext().getLog().info("Uptime: {}s", uptime / 1000);
        getContext().getLog().info("Tasks completed: {}", tasksCompleted);
        getContext().getLog().info("Total INDs found: {}", totalINDs);
        getContext().getLog().info("Avg IND/task: {}",
                tasksCompleted > 0 ? totalINDs / tasksCompleted : 0);
        getContext().getLog().info("Avg time/task: {}ms",
                tasksCompleted > 0 ? totalProcessingTime / tasksCompleted : 0);
        getContext().getLog().info("Memory used: {}/{} MB",
                heapUsage.getUsed() / (1024 * 1024),
                heapUsage.getMax() / (1024 * 1024));
        getContext().getLog().info("==========================");

        return this;
    }
}