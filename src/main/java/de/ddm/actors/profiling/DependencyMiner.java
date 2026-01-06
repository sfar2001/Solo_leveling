package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		int result;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class INDResultMessage implements Message {
		private static final long serialVersionUID = -7642425159675583600L;
		private ActorRef<DependencyWorker.Message> dependencyWorker;
		private String taskId;
		private List<InclusionDependency> discoveredINDs;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TableLoadedMessage implements Message {
		private static final long serialVersionUID = -5322425954432915839L;
		String tableName;
		boolean fullyLoaded;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);

		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();

		// Initialize data structures - USE DISK-BACKED OR LIMITED STORAGE
		this.tableHeaders = new ConcurrentHashMap<>();
		this.loadedTables = Collections.synchronizedSet(new HashSet<>());
		this.pendingTasks = new LinkedList<>();
		this.activeWorkers = new AtomicInteger(0);
		this.allTablesLoaded = false;

		// CRITICAL: Don't store all table data in memory!
		this.tableData = new ConcurrentHashMap<>();
		this.tableSamples = new ConcurrentHashMap<>(); // Store only samples

		// Configuration for large datasets
		this.maxTablesToProcess = Math.min(50, this.inputFiles.length); // Don't expect more tables than exist
		this.maxComparisons = 5000; // Very limited comparisons
		this.sampleSize = 5000; // Smaller sample size
		this.maxTableSizeInMemory = 500000; // Don't load tables larger than this

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));

		this.getContext().getLog().warn("Processing large dataset: {} tables. Will process only first {} tables.",
				inputFiles.length, maxTablesToProcess);

	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;
	private final AtomicInteger tasksCompleted = new AtomicInteger(0);
	private final AtomicInteger totalINDsDiscovered = new AtomicInteger(0);

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

	// Data storage - LIMITED!
	private final Map<String, String[]> tableHeaders;
	private final Map<String, List<String[]>> tableData; // Only for small tables
	private final Map<String, List<String[]>> tableSamples; // Samples for large tables
	private final Set<String> loadedTables;
	private final Queue<Task> pendingTasks;
	private final AtomicInteger activeWorkers;
	private boolean allTablesLoaded;

	// Memory limits
	private final int maxTablesToProcess;
	private final int maxComparisons;
	private final int sampleSize;
	private final int maxTableSizeInMemory;
	private int tablesLoaded = 0;

	@Data
	@AllArgsConstructor
	private static class Task {
		String sourceTable;
		String targetTable;
		String taskId;
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onMessage(INDResultMessage.class, this::handle)
				.onMessage(TableLoadedMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}
	private Behavior<Message> handle(StartMessage message) {
		this.getContext().getLog().warn("Starting IND discovery with {} tables. Processing only first {}.",
				inputFiles.length, maxTablesToProcess);

		// First get headers
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));

		// Then load data - but only for first N tables
		this.getContext().getLog().info("Loading data for first {} tables...", maxTablesToProcess);
		for (int i = 0; i < Math.min(maxTablesToProcess, inputReaders.size()); i++) {
			inputReaders.get(i).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));
		}

		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		String tableName = getTableName(message.getId());

		// Only process if we're within our limit
		if (message.getId() >= maxTablesToProcess) {
			this.getContext().getLog().debug("Skipping table {} (beyond limit {})",
					tableName, maxTablesToProcess);
			// Send empty batch to stop loading
			this.inputReaders.get(message.getId())
					.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 0));
			return this;
		}

		this.headerLines[message.getId()] = message.getHeader();
		this.tableHeaders.put(tableName, message.getHeader());

		this.getContext().getLog().debug("Got header for {}: {} columns",
				tableName, message.getHeader().length);
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		String tableName = getTableName(message.getId());

		// Skip if beyond limit
		if (message.getId() >= maxTablesToProcess) {
			return this;
		}

		if (!message.getBatch().isEmpty()) {
			// Check if table is getting too large
			int currentSize = this.tableData.getOrDefault(tableName, new ArrayList<>()).size();
			if (currentSize + message.getBatch().size() > maxTableSizeInMemory) {
				// Table is too large - only keep a sample
				this.getContext().getLog().warn("Table {} exceeds memory limit ({} rows). Keeping only sample.",
						tableName, currentSize + message.getBatch().size());

				// Create sample if not exists
				if (!this.tableSamples.containsKey(tableName)) {
					List<String[]> allData = new ArrayList<>();
					if (this.tableData.containsKey(tableName)) {
						allData.addAll(this.tableData.get(tableName));
					}
					allData.addAll(message.getBatch());

					// Take sample
					List<String[]> sample = smartSampleData(allData, sampleSize);
					this.tableSamples.put(tableName, sample);

					// Clear full data to save memory
					this.tableData.remove(tableName);
				}
			} else {
				// Store data normally
				this.tableData.computeIfAbsent(tableName, k -> new ArrayList<>())
						.addAll(message.getBatch());
			}

			this.getContext().getLog().debug("Added {} rows to {}",
					message.getBatch().size(), tableName);

			// Request next batch
			this.inputReaders.get(message.getId())
					.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));
		} else {
			// Table fully loaded
			tablesLoaded++;
			this.loadedTables.add(tableName);

			// Get final size
			int finalSize = 0;
			if (this.tableData.containsKey(tableName)) {
				finalSize = this.tableData.get(tableName).size();
			} else if (this.tableSamples.containsKey(tableName)) {
				finalSize = this.tableSamples.get(tableName).size();
			}

			this.getContext().getLog().info("Table {} fully loaded with {} rows ({}/{} tables loaded)",
					tableName, finalSize, tablesLoaded, Math.min(maxTablesToProcess, inputFiles.length));

			// Start processing when we have at least 2 tables
			if (tablesLoaded >= 2 && tablesLoaded <= maxTablesToProcess) {
				if (tablesLoaded == 2) {
					this.getContext().getLog().info("Starting IND discovery with {} tables...", tablesLoaded);
					generateSmartTasks();
					assignTasksToWorkers();
				} else if (tablesLoaded % 5 == 0 || tablesLoaded <= 10) { // More frequent for small datasets
					// Add more tasks as more tables load
					this.getContext().getLog().info("Adding more tasks with {} tables loaded", tablesLoaded);
					generateAdditionalTasks();
					assignTasksToWorkers();
				}
			}

			// Check if we've loaded all tables we plan to process
			int expectedTables = Math.min(this.inputFiles.length, this.maxTablesToProcess);
			if (tablesLoaded >= expectedTables) {
				this.allTablesLoaded = true;
				this.getContext().getLog().info("All {} tables loaded. Discovery in progress...", expectedTables);

				// If we only have 1 table, end immediately
				if (expectedTables < 2) {
					this.getContext().getLog().info("Only 1 table loaded. No comparisons possible.");
					this.end();
				}
			}
		}
		return this;
	}
	private void generateSmartTasks() {
		List<String> tables = new ArrayList<>(this.loadedTables);

		if (tables.size() < 2) {
			this.getContext().getLog().info("Only {} table(s) loaded. Need at least 2 for comparisons.",
					tables.size());
			return;
		}

		int comparisons = 0;

		// Generate tasks for all possible pairs
		for (int i = 0; i < tables.size() && comparisons < maxComparisons; i++) {
			for (int j = i + 1; j < tables.size() && comparisons < maxComparisons; j++) {
				String table1 = tables.get(i);
				String table2 = tables.get(j);

				// Get table sizes
				int size1 = getTableSize(table1);
				int size2 = getTableSize(table2);

				// Skip if sizes are extremely different (1000x ratio)
				if (size1 > 1000 && size2 > 1000 &&
						Math.max(size1, size2) > Math.min(size1, size2) * 1000) {
					continue;
				}

				// Generate both directions (A→B and B→A)
				this.pendingTasks.add(new Task(table1, table2,
						table1 + "_to_" + table2 + "_" + comparisons));
				comparisons++;

				if (comparisons < maxComparisons) {
					this.pendingTasks.add(new Task(table2, table1,
							table2 + "_to_" + table1 + "_" + comparisons));
					comparisons++;
				}
			}
		}

		this.getContext().getLog().info("Generated {} comparison tasks for {} tables",
				comparisons, tables.size());
	}
	private void generateAdditionalTasks() {
		List<String> allTables = new ArrayList<>(this.loadedTables);

		if (allTables.size() < 2) {
			return;
		}

		// Get tables that already have tasks
		Set<String> tablesWithTasks = new HashSet<>();
		for (Task task : this.pendingTasks) {
			tablesWithTasks.add(task.getSourceTable());
			tablesWithTasks.add(task.getTargetTable());
		}

		int comparisons = 0;

		// Add tasks between ALL tables (not just new vs existing)
		for (int i = 0; i < allTables.size() && comparisons < maxComparisons; i++) {
			for (int j = i + 1; j < allTables.size() && comparisons < maxComparisons; j++) {
				String table1 = allTables.get(i);
				String table2 = allTables.get(j);

				// Skip if this pair already has tasks in both directions
				boolean hasTask1 = false;
				boolean hasTask2 = false;
				for (Task task : this.pendingTasks) {
					if (task.getSourceTable().equals(table1) && task.getTargetTable().equals(table2)) {
						hasTask1 = true;
					}
					if (task.getSourceTable().equals(table2) && task.getTargetTable().equals(table1)) {
						hasTask2 = true;
					}
				}

				if (!hasTask1) {
					this.pendingTasks.add(new Task(table1, table2,
							table1 + "_to_" + table2 + "_add_" + comparisons));
					comparisons++;
				}

				if (comparisons < maxComparisons && !hasTask2) {
					this.pendingTasks.add(new Task(table2, table1,
							table2 + "_to_" + table1 + "_add_" + comparisons));
					comparisons++;
				}
			}
		}

		if (comparisons > 0) {
			this.getContext().getLog().info("Added {} new comparison tasks", comparisons);
		}
	}
	private int getTableSize(String tableName) {
		if (this.tableData.containsKey(tableName)) {
			return this.tableData.get(tableName).size();
		} else if (this.tableSamples.containsKey(tableName)) {
			return this.tableSamples.get(tableName).size();
		}
		return 0;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);

			this.getContext().getLog().info("Worker registered: {}",
					dependencyWorker.path().name());

			// 🔥 CRITICAL FIX: Assign task immediately when worker registers
			if (this.loadedTables.size() >= 2) { // Only if we have data
				assignTaskToWorker(dependencyWorker);
			}
		}
		return this;
	}
	private Behavior<Message> handle(CompletionMessage message) {
		this.getContext().getLog().debug("Received dummy completion from worker");
		// 🔥 CRITICAL: Assign real task
		assignTaskToWorker(message.getDependencyWorker());
		return this;
	}

	private Behavior<Message> handle(INDResultMessage message) {
		this.activeWorkers.decrementAndGet();

		this.getContext().getLog().info("📥 Received {} INDs from {}",
				message.getDiscoveredINDs().size(),
				message.getDependencyWorker().path().name());

		if (!message.getDiscoveredINDs().isEmpty()) {
			int indsCount = message.getDiscoveredINDs().size();
			this.totalINDsDiscovered.addAndGet(indsCount);

			this.resultCollector.tell(new ResultCollector.ResultMessage(
					message.getDiscoveredINDs()
			));
		}

		this.tasksCompleted.incrementAndGet();

		// Show progress
		this.getContext().getLog().info("📊 Progress: {} tasks completed, {} pending, {} INDs found",
				this.tasksCompleted.get(),
				this.pendingTasks.size(),
				this.totalINDsDiscovered.get());

		// Assign new task if available
		assignTaskToWorker(message.getDependencyWorker());

		// FIXED: Check completion for any number of tables
		int expectedTables = Math.min(this.inputFiles.length, this.maxTablesToProcess);
		boolean allExpectedTablesLoaded = this.tablesLoaded >= expectedTables;

		if (allExpectedTablesLoaded && this.pendingTasks.isEmpty() && this.activeWorkers.get() == 0) {
			this.getContext().getLog().info("🎉 All {} tasks completed! {} tables loaded.",
					this.tasksCompleted.get(), this.tablesLoaded);
			this.end();
		}

		return this;
	}	private void assignTasksToWorkers() {
		this.getContext().getLog().info("🔄 CHECKING: {} workers, {} pending tasks",
				this.dependencyWorkers.size(), this.pendingTasks.size());

		for (ActorRef<DependencyWorker.Message> worker : this.dependencyWorkers) {
			this.getContext().getLog().info("📤 Assigning task to worker: {}",
					worker.path().name());
			assignTaskToWorker(worker);
		}
	}

	private void assignTaskToWorker(ActorRef<DependencyWorker.Message> worker) {
		this.getContext().getLog().info("📋 TASK QUEUE STATUS: {} pending tasks, {} active workers",
				this.pendingTasks.size(), this.activeWorkers.get());
		if (!this.pendingTasks.isEmpty()) {
			Task task = this.pendingTasks.poll();
			this.activeWorkers.incrementAndGet();

			this.getContext().getLog().info("🎯 ASSIGNING TASK: {} → {} (task {})",
					task.getSourceTable(), task.getTargetTable(), task.getTaskId());
			// Get data for the tables
			List<String[]> sourceData = getTableData(task.getSourceTable());
			List<String[]> targetData = getTableData(task.getTargetTable());

			if (sourceData == null || targetData == null) {
				this.getContext().getLog().error("❌ Missing data for tables: {} or {}",
						task.getSourceTable(), task.getTargetTable());
				this.activeWorkers.decrementAndGet();
				return;
			}

			// Use appropriate sample size
			int srcSampleSize = Math.min(Math.max(sampleSize, sourceData.size() / 100), sourceData.size());
			int tgtSampleSize = Math.min(Math.max(sampleSize, targetData.size() / 100), targetData.size());

			List<String[]> sourceSample = smartSampleData(sourceData, srcSampleSize);
			List<String[]> targetSample = smartSampleData(targetData, tgtSampleSize);

			this.getContext().getLog().info("📊 Sample sizes: {} → {} ({} vs {} rows)",
					task.getSourceTable(), task.getTargetTable(),
					sourceSample.size(), targetSample.size());

			DependencyWorker.TableComparisonTask realTask = new DependencyWorker.TableComparisonTask(
					this.largeMessageProxy,
					task.getTaskId(),
					task.getSourceTable(),
					task.getTargetTable(),
					sourceSample,
					targetSample,
					this.tableHeaders.get(task.getSourceTable()),
					this.tableHeaders.get(task.getTargetTable())
			);

			worker.tell(realTask);
			this.getContext().getLog().info("✅ Task assigned to {}", worker.path().name());

		} else {
			this.getContext().getLog().info("⏸️ No tasks available for {}",
					worker.path().name());
		}
	}

	private List<String[]> getTableData(String tableName) {
		if (this.tableData.containsKey(tableName)) {
			return this.tableData.get(tableName);
		} else if (this.tableSamples.containsKey(tableName)) {
			return this.tableSamples.get(tableName);
		}
		return null;
	}

	private List<String[]> smartSampleData(List<String[]> fullData, int sampleSize) {
		if (fullData.size() <= sampleSize) {
			return fullData;
		}

		List<String[]> sample = new ArrayList<>(sampleSize);
		Random rand = new Random();

		// Reservoir sampling
		for (int i = 0; i < fullData.size(); i++) {
			if (i < sampleSize) {
				sample.add(fullData.get(i));
			} else {
				int j = rand.nextInt(i + 1);
				if (j < sampleSize) {
					sample.set(j, fullData.get(i));
				}
			}
		}

		return sample;
	}

	private void end() {
		this.getContext().getLog().info("🚀 Calling end() - sending FinalizeMessage to ResultCollector");
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining {} INDs within {} ms!",
				this.totalINDsDiscovered.get(), discoveryTime);
	}
	private String getTableName(int fileId) {
		return this.inputFiles[fileId].getName().replace(".csv", "");
	}

	private Behavior<Message> handle(TableLoadedMessage message) {
		return this;
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		this.getContext().getLog().info("Worker terminated: {}", dependencyWorker.path().name());
		return this;
	}
}