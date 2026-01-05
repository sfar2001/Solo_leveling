package de.ddm;

import akka.actor.typed.ActorSystem;
import com.fasterxml.jackson.databind.annotation.JsonAppend;
import de.ddm.actors.Guardian;
import de.ddm.configuration.Command;
import de.ddm.configuration.SystemConfiguration;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import java.io.IOException;
@Slf4j
@Getter
@Setter
public class Main {

	public static void main(String[] args) {
		Command.applyOn(args);

		SystemConfiguration config = SystemConfigurationSingleton.get();

		final ActorSystem<Guardian.Message> guardian = ActorSystem.create(Guardian.create(), config.getActorSystemName(), config.toAkkaConfig());

		if (config.getRole().equals(SystemConfiguration.MASTER_ROLE)) {
			if (config.isStartPaused())
				waitForInput(">>> Press ENTER to start <<<");

			guardian.tell(new Guardian.StartMessage());

			if (config.isEnterShutdown()) {
				waitForInput(">>> Press ENTER to exit <<<");
				guardian.tell(new Guardian.ShutdownMessage());
			}
		}
	}

	private static void waitForInput(String message) {
		try {
			System.out.println(message);
			System.in.read();
		} catch (IOException ignored) {
		}
	}
}
