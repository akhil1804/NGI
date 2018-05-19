const fs = require('fs');
const LineByLineReader = require('line-by-line');

const mdFileTemp = '/home/vassar/Downloads/CSV/New CSVs/Grid_IS_1_with_0.05_latlong_ele.csv';
const mdFileRF = '/home/vassar/Downloads/CSV/New CSVs/Grid_IS_0.25_with_0.05_latlong_0.05ele.csv';
// const bdFilesRFDir = '/home/vassar/Documents/Docs/Geoportal/Grid Interpolation/RF-Daily/';
const bdFilesRFDir = '/home/vassar/Documents/Docs/Geoportal/Grid Interpolation/RF-Daily(copy)/';     
const bdFilesTempDir = '/home/vassar/Documents/Docs/Geoportal/Grid Interpolation/TEMP-Daily(copy)/';
// const bdFilesTempDir = '/home/vassar/Documents/Docs/Geoportal/Grid Interpolation/TEMP-Daily/';
const outputFilesDir = '/home/vassar/Documents/Docs/Geoportal/Grid Interpolation/Data Files/';

/* 
    tempMD = {
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

let rfTempData = {};
let tempMD = {};
let rfMD = {};
let date;
let longList = [];
const bdFilesRFArr = fs.readdirSync(bdFilesRFDir);
const bdFilesTempArr = fs.readdirSync(bdFilesTempDir);

function promisifyLineByLineReader(lr, logic, options) {
    
    return new Promise(resolve => {
        
        lr.on('error', console.error);
        
        lr.on('line', line => logic(line, options));
        
        lr.on('end', () => {
            console.log(`File Closed`);
            resolve();
        });
    });
}

async function getBD(path, files, logic) {
    for(let i = 0; i < files.length; i++) {
        console.log("Reading", path + files[i]);
        await promisifyLineByLineReader(new LineByLineReader(path + files[i]), logic, files[i]);
    }
    return 1;
}

async function readData() {
    await promisifyLineByLineReader(new LineByLineReader(mdFileTemp), line => {
        let cell = line.trim().split(",");
        if(!parseFloat(cell[0])) return;
        let lat_lng1 = parseFloat(cell[2]).toFixed(3) + "##" + parseFloat(cell[1]).toFixed(3);
        if(!tempMD[lat_lng1]) {
            tempMD[lat_lng1] = {
                elev1: parseFloat(cell[3]),
                latlng05: {},
                value1: {}
            };
        }
        tempMD[lat_lng1]['latlng05'][parseFloat(cell[6]).toFixed(3) + "##" + parseFloat(cell[5]).toFixed(3)] = parseFloat(cell[7]);
    });
    await promisifyLineByLineReader(new LineByLineReader(mdFileRF), line => {
        let cell = line.trim().split(",");
        if(!parseFloat(cell[0])) return;
        let lat_lng25 = parseFloat(cell[1]).toFixed(3) + "##" + parseFloat(cell[2]).toFixed(3);
        if(!rfMD[lat_lng25]) {
            rfMD[lat_lng25] = {
                latlng05: [],
                value1: {}
            };
        }
        rfMD[lat_lng25]['latlng05'].push(parseFloat(cell[4]).toFixed(3) + "##" + parseFloat(cell[5]).toFixed(3));
    });
    await getBD(bdFilesRFDir, bdFilesRFArr, readIMD);
    await getBD(bdFilesTempDir, bdFilesTempArr, readTempIMD);
    final(); // Uncomment for computing and writing

}

// function onlyUnique(value, index, self) { return self.indexOf(value) === index;} // To be removed

function readIMD(line) {
    line = line.trim();
    if(!line) return;
    let cell = line.split(/\s+/);
    if(parseFloat(cell[0])>99999.0) {
        date = parseInt(cell[0]);
        longList = [];
        for(let i = 1; i < cell.length; i++) {
            longList.push(parseFloat(cell[i]).toFixed(3));
        }
    }else {
        let lat = parseFloat(cell[0]).toFixed(3);
        for(let i = 1; i < cell.length; i++) {
            let value = parseFloat(cell[i]);
            if(value > -99.0) {    			
                let latlngkey = lat + "##" + longList[i-1];
                if(rfMD[latlngkey]) {
                    if(!rfMD[latlngkey]['value1']) rfMD[latlngkey]['value1'] = {};
                    rfMD[latlngkey]['value1'][date] = value;
                }
            }
        }
    }    
}

function readTempIMD(line, file) {
    line = line.trim();
    if (!line || line.indexOf("DAILY") > -1) {
        return;
    }
    let type = file.substring(0, 3).toUpperCase();
    if(type !== "MAX" && type !== "MIN") return;
    let cell = line.split(/\s+/);
    if (cell[0] == "DTMTYEAR") {
        longList = [];
        for (let i = 2; i < cell.length; i++) longList.push(parseFloat(cell[i]).toFixed(3));
    } else {
        let dateString = convertDateFormat(cell[0]);
        let date = parseInt(dateString);
        let lat = parseFloat(cell[1]).toFixed(3);
        for (let i = 2; i < cell.length; i++) {
            let value = parseFloat(cell[i]);
            if (value < 99.90) {
                let latlngkey = lat + "##" + longList[i - 2];
                if(tempMD[latlngkey]) {
                    if(!tempMD[latlngkey]['value1'][date]) tempMD[latlngkey]['value1'][date] = {};
                    tempMD[latlngkey]['value1'][date][type] = value;
                }
            }
        }
    }      
}

function convertDateFormat(date) {
    if(date && date.length < 6) return;
    // Assumes that date is in ddMMyyyy format
    let formattedDate; // This is yyyyMMdd format
    formattedDate = date.substring(4);
    formattedDate += date.substring(2, 4);
    formattedDate += date.substring(0, 2);
    return formattedDate;
}

function final() {
    console.log("Temperature data computing...");
    for(let latLng in tempMD) {
        let elev1 = tempMD[latLng].elev1;
        let smallGrids = tempMD[latLng].latlng05;
        for(let latLon in smallGrids) {
            let elev05 = smallGrids[latLon];
            let dateValuesMap = tempMD[latLng].value1;
            for(let date in dateValuesMap) {
                rfTempData[latLon] = {};
                rfTempData[latLon][date] = {
                    MAX: (dateValuesMap[date].MAX - (elev05 - elev1) * 6.5 / 1000).toFixed(2),
                    MIN: (dateValuesMap[date].MIN - (elev05 - elev1) * 6.5 / 1000).toFixed(2)
                }
            }
        }
    }

    console.log("Rainfall data computing...");
    for(let latLng in rfMD) {
        let smallGrids = rfMD[latLng].latlng05;
        for(let i = 0; i < smallGrids.length; i++) {
            let dateValuesMap = rfMD[latLng].value1;
            let latLon = smallGrids[i];
            for(let date in dateValuesMap) {
                if(!rfTempData[latLon]) rfTempData[latLon] = {};
                if(!rfTempData[latLon][date]) rfTempData[latLon][date] = {};
                rfTempData[latLon][date].RF = parseFloat(dateValuesMap[date]).toFixed(2);
            }
        }
    }

    console.log("Begin writing...");
    // MIN MAX RF 0 0     // File format
    for(let latLong in rfTempData) {
        let fileName = "data_" + latLong.replace("##", "_");
        let dateValuesMap = rfTempData[latLong];
        let dates = Object.keys(dateValuesMap).sort();
        console.log("Writing file", fileName, dates.length, "entries");
        for(let i = 0; i < dates.length; i++) {
            let lineString = '';
            let data = dateValuesMap[dates[i]];
            lineString += (data.MIN || '0');
            lineString += " ";
            lineString += (data.MAX || '0');
            lineString += " ";
            lineString += (data.RF || '0');
            lineString += " 0 0" + '\n';
            fs.appendFileSync(outputFilesDir + fileName, lineString, 'utf8');
        }
    }
}

readData();
