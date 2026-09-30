package tripwire.tripwire;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@org.springframework.scheduling.annotation.EnableScheduling
public class TripwireApplication {

	public static void main(String[] args) {
		SpringApplication.run(TripwireApplication.class, args);
	}

}
