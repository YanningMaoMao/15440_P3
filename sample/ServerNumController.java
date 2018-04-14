import java.util.HashMap;
import java.util.Map;

/**
 * This class contains methods that determines the optimal number of servers to start with
 * for each specific rate.
 * 
 * @author YanningMao <yanningm@andrew.cmu.edu>
 *
 */
public class ServerNumController {
	
	// stores the optimal number of servers for each request arrival rate
	private static Map<Double, Integer> serverNumChart = initServerNumChart();
	
	/**
	 * Initializes the optimal server chart.
	 * 
	 * @return chart, the initialized chart
	 */
	private static Map<Double, Integer> initServerNumChart() {
		Map<Double, Integer> chart = new HashMap<>();
		chart.put(0.25, 2);
		chart.put(0.5, 3);
		chart.put(0.75, 5);
		chart.put(1.0, 5);
		chart.put(1.25, 6);
		chart.put(1.5, 9);
		return chart;
	}
	
	/**
	 * Gets the optimal number of servers given the specific request arrival rate.
	 * 
	 * @param rate, request arrival rate
	 * @return num, the optimal number of servers to start with
	 */
	public static int getServerNum(double rate) {
		
		if (rate <= 0) {
			return 1;
		}
		else if (rate <= 0.25) {
			return serverNumChart.get(0.25);
		}
		else if (rate <= 0.5) {
			return serverNumChart.get(0.5);
		}
		else if (rate <= 0.75) {
			return serverNumChart.get(0.75);
		}
		else if (rate <= 1.0) {
			return serverNumChart.get(1.0);
		}
		else if (rate <= 1.25) {

			return serverNumChart.get(1.25);
		}
		else {

			return serverNumChart.get(1.5);
		}
	}
}

