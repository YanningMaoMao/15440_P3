/**
 * This class stores the result of benchmarking on the optimal number of servers to start with
 * for each hour.
 * 
 * @author YanningMao <yanningm@andrew.cmu.edu>
 *
 */
public class HourArrivalRate {
	
	// smallest hour number
	private static final int MIN_HOUR = 0;
	// largest hour number
	private static final int MAX_HOUR = 23;

	// the arrival rates for the hours
	private static double[] rates = {0.5, 0.3, 0.1, 0.1, 0.1, 0.2,
			  		 0.3, 0.6, 1.0, 0.8, 0.8, 0.8,
			  		 1.0, 1.1, 1.0, 0.8, 0.7, 0.8,
			  		 1.0, 1.2, 1.5, 1.4, 1.0, 0.8};
	
	/**
	 * Gets the arrival rate of the given hour.
	 * 
	 * @param hour, the hour number
	 * @return rate, the arrival rate
	 */
	public static double getHourRate(int hour) {
		if (hour < MIN_HOUR || hour > MAX_HOUR) {
			return -1;
		}
		else {
			return rates[hour];
		}
	}
}

