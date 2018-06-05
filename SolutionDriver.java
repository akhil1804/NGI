import java.io.*;
import java.util.*;
import java.lang.*;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
/**
 * tempMD = {
        $1_lat##lng : {
            elev1: $elev1,
            latlng05: {$05_lat##lng: $elev05},
            value1: { $date: {MAX: $max_Temp_1, MIN: $min_Temp_1}}
        }
    }

    rfMD = {
        $0.25_lat##lng : {
            latlng05: [$05_lat##lng],
            value1: { $date: $RF_0.25}
        }  
    }

    rfTempData = {
        $05_lat##lng: {
            $date: {MAX: $max_Temp, MIN: $min_Temp, RF: $RF}
        }
    }
 */
    

/**
 * TempMD
 */
class TempMD {

    Double elev1;
    Map<String, Double> latlng05;
    Map<String, Map<String, Double>> value1;
    public TempMD(Double elev1) {
        this.elev1 = elev1;
        latlng05 = new HashMap<>();
        value1 = new HashMap<>();
    }
    public Double getElev1() { return elev1; }
    public Map<String, Double> getLatLng05() { return latlng05;}
    public Map<String, Map<String, Double>> getValue1() { return value1; }
    public boolean putLatLng05(String latlng, Double elev05) {
        if(latlng05.get(latlng) == null) {
            latlng05.put(latlng, elev05);
            return true;
        }
        return false;
    }
    public boolean putDateData(String date, String type, Double val) {
        if(value1.get(date) == null) {
            value1.put(date, new HashMap<>());
        }
        value1.get(date).put(type, val);
        return true;
    }

    public static String toString(TempMD obj) {
        return "Elev=" + obj.getElev1() + " length of latlng05 =" + obj.getLatLng05().size()
            + " Length of values = " + obj.getValue1().size();
    }

    public int numberOfLatLon05() {
        return latlng05.size();
    }
}

/**
 * RFMD
 */
class RFMD {

    List<String> latlng05;
    Map<String, Double> value1;
    public RFMD() {
        latlng05 = new ArrayList<>();
        value1 = new HashMap<>();
    }
    public List<String> getLatLng05() { return latlng05;}
    public Map<String, Double> getValue1() { return value1; }
    public boolean addLatLng05(String latlng) {
        if(latlng05.contains(latlng)) {
            return false;
        }
        latlng05.add(latlng);
        return true;
    }
    public boolean putDateData(String date, Double rf) {
        if(value1.get(date) == null) {
            value1.put(date, rf);
            return true;
        }
        return false;
    }

    public static String toString(RFMD obj) {
        return "length of latlng05 =" + obj.getLatLng05().size()
            + " Length of values = " + obj.getValue1().size();
    }

    public int numberOfLatLon05() {
        return latlng05.size();
    }
}

public class SolutionDriver {
    static final String mdFileTemp = "CSV/Grid_IS_1_with_0.05_latlong_ele.csv";
    static final String mdFileRF = "CSV/Grid_IS_0.25_with_0.05_latlong_0.05ele.csv";
    static final String bdFilesRFDir = "Business Data/RF-Daily (copy)/";     
    static final String bdFilesTempDir = "Business Data/TEMP-Daily (copy)/";
    static final String outputFilesDir = "Result Files/";
    static Map<String, Map<String, Map<String, Double>>> rfTempData;
    static Map<String, TempMD> tempMD;
    static Map<String, RFMD> rfMD;
    public static void main(String[] args) {
        try {
            File[] rfFiles, tmpFiles;
            List<String> maxTempArr =  new ArrayList<>();
            List<String> minTempArr =  new ArrayList<>();
            tmpFiles = new File(bdFilesTempDir).listFiles();
            rfFiles = new File(bdFilesRFDir).listFiles();

            Arrays.sort(tmpFiles, (f1, f2) -> {
                // System.out.println(f1.getName().split("_")[1]);
                String file1 = f1.getName().split("_")[1].split("\\.")[0];
                String file2 = f2.getName().split("_")[1].split("\\.")[0];
                return Integer.parseInt(file1) - Integer.parseInt(file2);
            });
            
            Arrays.sort(rfFiles, (f1, f2) -> {
                String file1 = f1.getName().substring(3, 7);
                String file2 = f2.getName().substring(3, 7);
                return Integer.parseInt(file1) - Integer.parseInt(file2);
            });

            Arrays.stream(tmpFiles).forEach(file -> {
                String fileName = file.getName();
                if(fileName.substring(0, 3).equalsIgnoreCase("MAX")) maxTempArr.add(fileName);
                else if(fileName.substring(0, 3).equalsIgnoreCase("MIN")) minTempArr.add(fileName);
            });

            System.out.println(maxTempArr);
            System.out.println(minTempArr);
            int i = 0;
            while (i < rfFiles.length) {
                rfTempData = new HashMap<>();
                tempMD = new HashMap<>();
                rfMD = new HashMap<>();
                System.out.println("Readind RF CSV file");
                readRFCSV(mdFileRF);
                System.out.println("Readind Temp CSV file");
                readTempCSV(mdFileTemp);
                System.out.println("Readind RF BD file - " + rfFiles[i].getName());
                parseIMDFile(bdFilesRFDir, rfFiles[i].getName());
                System.out.println("Readind Temp BD file - " + maxTempArr.get(i) + " & " + minTempArr.get(i));
                parseIMDTempFile(bdFilesTempDir, maxTempArr.get(i));
                parseIMDTempFile(bdFilesTempDir, minTempArr.get(i));
                computeTempData();
                computeRFData();
                writeComputedData();  
                System.gc();   
                i++;       
            }
             
        } catch(IOException | ParseException e) {
            e.printStackTrace();
            // System.out.println();
        }
    }

    public static void parseIMDFile(String rfFileDir, String fileName) throws IOException {
		
		List<String> longList = new ArrayList<String>();
    	Integer date = null;
    	
		BufferedReader br = new BufferedReader(new FileReader(rfFileDir + fileName));
	    for(String line; (line = br.readLine()) != null; ) {
	    	// process the line.
	    	line=line.trim();
	    	if(line.isEmpty()) {
	    		continue;
	    	}
	    	
	    	String[] cell = line.split("\\s+");

	    	if(Double.parseDouble(cell[0]) > 99999.0) {
	    		date = Integer.parseInt(cell[0]);
	    		longList.clear();
	    		for(int i = 1; i < cell.length; i++) {
	    			longList.add(String.format("%.3f", Double.parseDouble(cell[i])));
		    	}
	    	}else {
	    		String lat = String.format("%.3f", Double.parseDouble(cell[0]));
		    	for(int i = 1; i < cell.length; i++) {
		    		Double value = Double.parseDouble(cell[i]);
                    String latlngkey = lat + "##" + longList.get(i-1);
                    RFMD obj = rfMD.get(latlngkey);
                    if( obj != null) {
                        obj.putDateData(Integer.toString(date), (value > -99) ? value : 0);
                    }
		    	}
	    	}
        }
        br.close();
    }
    
    public static void parseIMDTempFile(String tempFileDir, String fileName) throws IOException, ParseException {

		List<String> longList = new ArrayList<String>();
        Integer date = null;
        String type = fileName.substring(0, 3).toUpperCase();
		BufferedReader br = new BufferedReader(new FileReader(tempFileDir + fileName));
	    for(String line; (line = br.readLine()) != null; ) {
	    	// process the line.
	    	line=line.trim();
	    	if(line.isEmpty() || line.contains("DAILY")) {
	    		continue;
	    	}
	    	String[] cell = line.split("\\s+");

	    	if(cell[0].equals("DTMTYEAR")) {
	    		longList.clear();
	    		for(int i=2;i<cell.length;i++) {
	    			longList.add(String.format("%.3f", Double.parseDouble(cell[i])));
		    	}
	    	}else {
	    		String dateString = convertDateFormat(cell[0], "ddMMyyyy");
	    		date = Integer.parseInt(dateString);
	    		String lat = String.format("%.3f", Double.parseDouble(cell[1]));
		    	for(int i = 2; i < cell.length; i++) {
                    Double value = Double.parseDouble(cell[i]);  
                     			
                    String latlngkey = lat + "##" + longList.get(i-2);
                    TempMD obj = tempMD.get(latlngkey);
                    if( obj != null) {
                        obj.putDateData(Integer.toString(date), type, (value < 99.90) ? value : 0);
                    }
		    	}
	    	}
        }
        br.close();
    }
    
    public static void readRFCSV(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String lat_lng1 = "";
        for(String line; (line = br.readLine()) != null; ) {
            line=line.trim();
	    	if(line.isEmpty()) {
	    		continue;
	    	}
            String[] cell = line.split(",");
            try {
                Double.parseDouble(cell[0]);
                lat_lng1 = String.format("%.3f", Double.parseDouble(cell[1])) + "##" + String.format("%.3f", Double.parseDouble(cell[2]));
                if(rfMD.get(lat_lng1) == null) {
                    rfMD.put(lat_lng1, new RFMD());
                }
                RFMD obj = rfMD.get(lat_lng1);
                String ltln = String.format("%.3f", Double.parseDouble(cell[4])) + "##" + String.format("%.3f", Double.parseDouble(cell[5]));
                obj.addLatLng05(ltln);
            } catch(NumberFormatException e) {}
        }
        br.close();
    }

    public static void readTempCSV(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String lat_lng1 = "";
        for(String line; (line = br.readLine()) != null; ) {
            line=line.trim();
	    	if(line.isEmpty()) {
	    		continue;
	    	}
            String[] cell = line.split(",");
            try {
                Double.parseDouble(cell[0]);
                lat_lng1 = String.format("%.3f", Double.parseDouble(cell[2])) + "##" + String.format("%.3f", Double.parseDouble(cell[1]));
                if(tempMD.get(lat_lng1) == null) {
                    tempMD.put(lat_lng1, new TempMD(Double.parseDouble(cell[3])));
                }
                TempMD obj = tempMD.get(lat_lng1);
                String ltln = String.format("%.3f", Double.parseDouble(cell[6])) + "##" + String.format("%.3f", Double.parseDouble(cell[5]));
                obj.putLatLng05(ltln, Double.parseDouble(cell[7]));
            } catch(NumberFormatException e) {}
        }
        br.close();
    }

    private static String convertDateFormat(String date,String dateformat) throws ParseException {
		DateFormat format = new SimpleDateFormat(dateformat);
		Date dateobj = format.parse(date);
		DateFormat format2 = new SimpleDateFormat("yyyyMMdd");
		String finaldate = format2.format(dateobj);
		return finaldate;
    }
    
    private static void computeTempData() {
        int tmpCount = 0;
        System.out.println("Temperature data computing...");
        for(String latLng : tempMD.keySet()) {
            TempMD obj = tempMD.get(latLng);
            double elev1 = obj.getElev1();
            Map<String, Double> smallGrids = obj.getLatLng05();
            tmpCount += smallGrids.keySet().size();
            for(String latLon : smallGrids.keySet()) {
                double elev05 = smallGrids.get(latLon);
                Map<String, Map<String, Double>> dateValuesMap = obj.getValue1();
                if(rfTempData.get(latLon) == null) rfTempData.put(latLon, new HashMap<>());
                for(String date : dateValuesMap.keySet()) {
                    if(rfTempData.get(latLon).get(date) == null) {
                        Double maxVal = dateValuesMap.get(date).get("MAX") == null 
                            ? 0 
                            : (dateValuesMap.get(date).get("MAX") - (elev05 - elev1) * 6.5 / 1000);
                        Double minVal = dateValuesMap.get(date).get("MIN") == null 
                            ? 0 
                            : (dateValuesMap.get(date).get("MIN") - (elev05 - elev1) * 6.5 / 1000);
                        Map<String, Double> max = new HashMap<>();
                        max.put("MAX", maxVal);
                        max.put("MIN", minVal);
                        rfTempData.get(latLon).put(date, max);
                    }
                }
            }
        }
        System.out.println("Data computed for " + rfTempData.keySet().size() + " ( " + tmpCount + " ) " + " grids..");
    }

    private static void computeRFData() {
        System.out.println("Rainfall data computing...");
        for(String latLng : rfMD.keySet()) {
            RFMD obj = rfMD.get(latLng);
            List<String> smallGrids = obj.getLatLng05();
            for(String latLon : smallGrids) {
                Map<String, Double> dateValuesMap = obj.getValue1();
                if(rfTempData.get(latLon) == null) rfTempData.put(latLon, new HashMap<>());
                for(String date : dateValuesMap.keySet()) {
                    if(rfTempData.get(latLon).get(date) == null) {
                        rfTempData.get(latLon).put(date, new HashMap<>());
                    }
                    rfTempData.get(latLon).get(date).put("RF", dateValuesMap.get(date));
                }
            }
        }
    }

    private static void writeComputedData() {
        System.out.println("Begin writing...");
            
        BufferedWriter bw = null;
        // MIN MAX RF 0 0     // File format
    
        for(String latLong : rfTempData.keySet()) {
            String fileName = "data_" + latLong.replace("##", "_");
            Map<String, Map<String, Double>> dateValuesMap = rfTempData.get(latLong);
            List<String> dates = new ArrayList<>(dateValuesMap.keySet());
            Collections.sort(dates);
            System.out.println("Writing file " + fileName + " " + dates.size() + " entries");
            if(dates.size() < 200) break;
            try {
                bw = new BufferedWriter(new FileWriter(outputFilesDir + fileName, true));
                for(String date: dates) {
                    String lineString = "";
                    Map<String, Double> data = dateValuesMap.get(date);
                    lineString += data.get("MIN") == null ? "0" : String.format("%.2f", data.get("MIN"));
                    lineString += " ";
                    lineString += data.get("MAX") == null ? "0" : String.format("%.2f", data.get("MAX"));
                    lineString += " ";
                    lineString += data.get("RF") == null ? "0" : String.format("%.2f", data.get("RF"));
                    lineString += " 0 0";
                    bw.write(lineString);
                    bw.newLine();
                    bw.flush();
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            } finally {                       // always close the file
                if (bw != null) try {
                bw.close();
                } catch (IOException ioe2) {
                // just ignore it
                }
            }
        }
    }
}