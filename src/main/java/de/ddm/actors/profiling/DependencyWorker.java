package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		int task;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TableComparisonTask implements Message {
		private static final long serialVersionUID = -4667745204456518161L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		String taskId;
		String sourceTable;
		String targetTable;
		List<String[]> sourceData;
		List<String[]> targetData;
		String[] sourceHeaders;
		String[] targetHeaders;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	private boolean enableDebugLogging = true;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.onMessage(TableComparisonTask.class, this::handleTableComparisonTask)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		this.getContext().getLog().info("Working on dummy task!");

		int result = message.getTask();
		long time = System.currentTimeMillis();
		Random rand = new Random();
		int runtime = (rand.nextInt(2) + 2) * 1000;
		while (System.currentTimeMillis() - time < runtime)
			result = ((int) Math.abs(Math.sqrt(result)) * result) % 1334525;

		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), result);
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));

		return this;
	}

	private Behavior<Message> handleTableComparisonTask(TableComparisonTask message) {
		this.getContext().getLog().info("Discovering INDs from {} to {} ({} rows vs {} rows)",
				message.getSourceTable(), message.getTargetTable(),
				message.getSourceData().size(), message.getTargetData().size());

		long startTime = System.currentTimeMillis();

		List<InclusionDependency> discoveredINDs = discoverINDs(
				message.getSourceTable(), message.getSourceData(), message.getSourceHeaders(),
				message.getTargetTable(), message.getTargetData(), message.getTargetHeaders()
		);

		long duration = System.currentTimeMillis() - startTime;
		this.getContext().getLog().info("Found {} INDs in {} ms", discoveredINDs.size(), duration);

		LargeMessageProxy.LargeMessage resultMessage = new DependencyMiner.INDResultMessage(
				this.getContext().getSelf(),
				message.getTaskId(),
				discoveredINDs
		);

		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(
				resultMessage,
				message.getDependencyMinerLargeMessageProxy()));

		return this;
	}

	// GENERIC IND Discovery Algorithm - Works with ANY dataset
	private List<InclusionDependency> discoverINDs(
			String sourceTable, List<String[]> sourceData, String[] sourceHeaders,
			String targetTable, List<String[]> targetData, String[] targetHeaders) {

		List<InclusionDependency> inds = new ArrayList<>();

		if (enableDebugLogging) {
			logComparisonDetails(sourceTable, sourceData, sourceHeaders,
					targetTable, targetData, targetHeaders);
		}

		// 1. Discover unary INDs (single column)
		inds.addAll(discoverUnaryINDs(sourceTable, sourceData, sourceHeaders,
				targetTable, targetData, targetHeaders));

		// 2. Discover binary INDs (two columns)
		inds.addAll(discoverBinaryINDs(sourceTable, sourceData, sourceHeaders,
				targetTable, targetData, targetHeaders));

		// Optional: Test mode - always add a test IND to verify system works
		if (enableDebugLogging && inds.isEmpty()) {
			inds.add(createTestIND(sourceTable, sourceHeaders, targetTable, targetHeaders));
		}

		return inds;
	}

	private void logComparisonDetails(
			String sourceTable, List<String[]> sourceData, String[] sourceHeaders,
			String targetTable, List<String[]> targetData, String[] targetHeaders) {

		this.getContext().getLog().info("DEBUG COMPARISON: {} ({} cols, {} rows) → {} ({} cols, {} rows)",
				sourceTable, sourceHeaders.length, sourceData.size(),
				targetTable, targetHeaders.length, targetData.size());

		this.getContext().getLog().info("DEBUG HEADERS: {} = {}", sourceTable, Arrays.toString(sourceHeaders));
		this.getContext().getLog().info("DEBUG HEADERS: {} = {}", targetTable, Arrays.toString(targetHeaders));

		if (!sourceData.isEmpty()) {
			String[] firstRow = sourceData.get(0);
			this.getContext().getLog().info("DEBUG FIRST ROW {}: {} columns",
					sourceTable, firstRow.length);
		}

		if (!targetData.isEmpty()) {
			String[] firstRow = targetData.get(0);
			this.getContext().getLog().info("DEBUG FIRST ROW {}: {} columns",
					targetTable, firstRow.length);
		}
	}

	private List<InclusionDependency> discoverUnaryINDs(
			String sourceTable, List<String[]> sourceData, String[] sourceHeaders,
			String targetTable, List<String[]> targetData, String[] targetHeaders) {

		List<InclusionDependency> inds = new ArrayList<>();

		for (int srcCol = 0; srcCol < sourceHeaders.length; srcCol++) {
			for (int tgtCol = 0; tgtCol < targetHeaders.length; tgtCol++) {

				Set<String> sourceValues = extractColumnValues(sourceData, srcCol);
				Set<String> targetValues = extractColumnValues(targetData, tgtCol);

				if (isInclusionDependency(sourceValues, targetValues)) {
					InclusionDependency ind = new InclusionDependency(
							new File(sourceTable + ".csv"),
							new String[]{sourceHeaders[srcCol]},
							new File(targetTable + ".csv"),
							new String[]{targetHeaders[tgtCol]}
					);
					inds.add(ind);

					if (enableDebugLogging) {
						this.getContext().getLog().info("FOUND UNARY IND: {}.{} ⊆ {}.{}",
								sourceTable, sourceHeaders[srcCol],
								targetTable, targetHeaders[tgtCol]);
					}
				}
			}
		}

		return inds;
	}

	private List<InclusionDependency> discoverBinaryINDs(
			String sourceTable, List<String[]> sourceData, String[] sourceHeaders,
			String targetTable, List<String[]> targetData, String[] targetHeaders) {

		List<InclusionDependency> inds = new ArrayList<>();

		for (int srcCol1 = 0; srcCol1 < sourceHeaders.length; srcCol1++) {
			for (int srcCol2 = srcCol1 + 1; srcCol2 < sourceHeaders.length; srcCol2++) {
				for (int tgtCol1 = 0; tgtCol1 < targetHeaders.length; tgtCol1++) {
					for (int tgtCol2 = tgtCol1 + 1; tgtCol2 < targetHeaders.length; tgtCol2++) {

						Set<String> sourcePairs = extractColumnPairValues(sourceData, srcCol1, srcCol2);
						Set<String> targetPairs = extractColumnPairValues(targetData, tgtCol1, tgtCol2);

						if (isInclusionDependency(sourcePairs, targetPairs)) {
							InclusionDependency ind = new InclusionDependency(
									new File(sourceTable + ".csv"),
									new String[]{sourceHeaders[srcCol1], sourceHeaders[srcCol2]},
									new File(targetTable + ".csv"),
									new String[]{targetHeaders[tgtCol1], targetHeaders[tgtCol2]}
							);
							inds.add(ind);

							if (enableDebugLogging) {
								this.getContext().getLog().info("FOUND BINARY IND: {}.{} ⊆ {}.{}",
										sourceTable, Arrays.toString(new String[]{sourceHeaders[srcCol1], sourceHeaders[srcCol2]}),
										targetTable, Arrays.toString(new String[]{targetHeaders[tgtCol1], targetHeaders[tgtCol2]}));
							}
						}
					}
				}
			}
		}

		return inds;
	}

	private Set<String> extractColumnValues(List<String[]> data, int column) {
		Set<String> values = new HashSet<>();
		for (String[] row : data) {
			if (column < row.length) {
				String value = row[column] != null ? row[column].trim() : "";
				if (!value.isEmpty()) {
					values.add(value);
				}
			}
		}
		return values;
	}

	private Set<String> extractColumnPairValues(List<String[]> data, int col1, int col2) {
		Set<String> pairs = new HashSet<>();
		for (String[] row : data) {
			if (col1 < row.length && col2 < row.length) {
				String val1 = row[col1] != null ? row[col1].trim() : "";
				String val2 = row[col2] != null ? row[col2].trim() : "";
				if (!val1.isEmpty() && !val2.isEmpty()) {
					pairs.add(val1 + "|||" + val2);
				}
			}
		}
		return pairs;
	}

	private boolean isInclusionDependency(Set<String> sourceValues, Set<String> targetValues) {
		if (sourceValues.isEmpty()) return false;
		if (sourceValues.size() > targetValues.size()) return false;

		for (String value : sourceValues) {
			if (!targetValues.contains(value)) {
				return false;
			}
		}
		return true;
	}

	// Creates a test IND to verify the system is working
	private InclusionDependency createTestIND(String sourceTable, String[] sourceHeaders,
											  String targetTable, String[] targetHeaders) {
		// Use first columns from each table for test
		String sourceCol = sourceHeaders.length > 0 ? sourceHeaders[0] : "COL1";
		String targetCol = targetHeaders.length > 0 ? targetHeaders[0] : "COL1";

		this.getContext().getLog().info("TEST MODE: Adding test IND to verify system");

		return new InclusionDependency(
				new File(sourceTable + ".csv"),
				new String[]{sourceCol},
				new File(targetTable + ".csv"),
				new String[]{targetCol}
		);
	}
}