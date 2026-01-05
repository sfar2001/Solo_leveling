package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.patterns.Reaper;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Worker extends AbstractBehavior<Worker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}

	// ADD THESE NEW MESSAGE CLASSES:
	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class WorkMessage implements Message, Serializable {
		private static final long serialVersionUID = 1L;
		private int task;
		private List<Integer> results = new ArrayList<>(); // If you want to return results
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RewardMessage implements Message, Serializable {
		private static final long serialVersionUID = 2L;
		private String reward;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "worker";

	public static Behavior<Message> create() {
		return Behaviors.setup(Worker::new);
	}

	private Worker(ActorContext<Message> context) {
		super(context);
		Reaper.watchWithDefaultReaper(this.getContext().getSelf());

		final int numWorkers = SystemConfigurationSingleton.get().getNumWorkers();

		this.workers = new ArrayList<>(numWorkers);
		for (int id = 0; id < numWorkers; id++)
			this.workers.add(context.spawn(
					DependencyWorker.create(),
					DependencyWorker.DEFAULT_NAME + "_" + id,
					DispatcherSelector.fromConfig("akka.worker-pool-dispatcher")));
	}

	/////////////////
	// Actor State //
	/////////////////

	final List<ActorRef<DependencyWorker.Message>> workers;
	private final List<Integer> results = new ArrayList<>(); // ADD THIS for storing results

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(WorkMessage.class, this::handle)    // ADD THIS
				.onMessage(RewardMessage.class, this::handle)  // ADD THIS
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	// ADD THESE NEW HANDLER METHODS:
	private Behavior<Message> handle(WorkMessage message) {
		this.getContext().getLog().info("Processing work task of size: {}", message.getTask());

		for (int i = 0; i < message.getTask(); i++) {
			this.results.add(i * i); // Store squared results
		}

		this.getContext().getLog().info("Did my work! Calculated {} squares.", message.getTask());

		// Optionally, you could send results back to sender
		// ActorRef<SomeMessage> sender = message.getReplyTo();
		// sender.tell(new ResultMessage(this.results));

		return this;
	}

	private Behavior<Message> handle(RewardMessage message) {
		this.getContext().getLog().info("Thanks for the '{}' reward!", message.getReward());
		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		this.getContext().getLog().info("Worker shutting down...");
		// If we expect the system to still be active when the a ShutdownMessage is issued,
		// we should propagate this ShutdownMessage to all active child actors so that they
		// can end their protocols in a clean way. Simply stopping this actor also stops all
		// child actors, but in a hard way!

		// Propagate shutdown to all DependencyWorker children
		for (ActorRef<DependencyWorker.Message> worker : this.workers) {
			//worker.tell(new DependencyWorker.ShutdownMessage());
		}

		return Behaviors.stopped();
	}
}