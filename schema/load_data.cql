USE Test;

INSERT INTO Users (companyName, name, phone, street) VALUES ('PP', 'Adam', 720, 'A St');
INSERT INTO Users (companyName, name, phone, street) VALUES ('PP', 'Ewa', 720, 'B St');
INSERT INTO Users (companyName, name, phone, street) VALUES ('PP', 'Ewa', 609, 'X St');     
INSERT INTO Users (companyName, name, phone, street) VALUES ('UAM', 'Ola', 507, 'B St');
INSERT INTO Users (companyName, name, phone, street) VALUES ('PP', 'Marek', 720, 'C St');     

INSERT INTO Users (companyName, name, pets) VALUES ('UAM', 'Ania', [ 'Tom', 'Jerry' ]);

INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-01 12:00:00', 13.2, 978.5);
INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-01 12:00:00', 16.6, 798.6);
INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-01 12:00:00', 15.0, 891.6);

INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-01 13:00:00', 13.5, 979.0);
INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-01 13:00:00', 16.7, 798.7);
INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-01 13:00:00', 15.5, 891.7);

INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-01 14:00:00', 14.1, 979.2);
INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-01 14:00:00', 17.2, 798.0);
INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-01 14:00:00', 16.1, 891.8);

INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-01 15:00:00', 14.0, 979.3);
INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-01 15:00:00', 17.1, 798.0);
INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-01 15:00:00', 15.9, 892.0);

INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-01 16:00:00', 13, 979);
INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-01 16:00:00', 16, 798);
INSERT INTO MeteoData (stationName, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-01 16:00:00', 15, 892);


INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-01', '2012-07-01 12:00:00', 13.2, 978.5);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-01', '2012-07-01 12:00:00', 16.6, 798.6);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-01', '2012-07-01 12:00:00', 15.0, 891.6);

INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-01', '2012-07-01 13:00:00', 13.5, 979.0);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-01', '2012-07-01 13:00:00', 16.7, 798.7);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-01', '2012-07-01 13:00:00', 15.5, 891.7);

INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-01', '2012-07-01 14:00:00', 14.1, 979.2);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-01', '2012-07-01 14:00:00', 17.2, 798.0);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-01', '2012-07-01 14:00:00', 16.1, 891.8);

INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-01', '2012-07-01 15:00:00', 14.0, 979.3);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-01', '2012-07-01 15:00:00', 17.1, 798.0);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-01', '2012-07-01 15:00:00', 15.9, 892.0);

INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-01', '2012-07-01 16:00:00', 13, 979);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-01', '2012-07-01 16:00:00', 16, 798);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-01', '2012-07-01 16:00:00', 15, 892);


INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-02', '2012-07-02 12:00:00', 14.2, 979.5);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-02', '2012-07-02 12:00:00', 17.6, 799.6);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-02', '2012-07-02 12:00:00', 16.0, 892.6);

INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-02', '2012-07-02 13:00:00', 14.5, 980.0);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-02', '2012-07-02 13:00:00', 17.7, 799.7);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-02', '2012-07-02 13:00:00', 16.5, 892.7);

INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-02', '2012-07-02 14:00:00', 15.1, 980.2);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-02', '2012-07-02 14:00:00', 18.2, 799.0);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-02', '2012-07-02 14:00:00', 17.1, 892.8);

INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-02', '2012-07-02 15:00:00', 15.0, 980.3);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-02', '2012-07-02 15:00:00', 18.1, 799.0);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-02', '2012-07-02 15:00:00', 16.9, 893.0);

INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Zakopane', '2012-07-02', '2012-07-02 16:00:00', 14, 980);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Kasprowy', '2012-07-02', '2012-07-02 16:00:00', 17, 799);
INSERT INTO MeteoData2 (stationName, mDay, measurement, temperature, pressure) VALUES ('Gubalowka', '2012-07-02', '2012-07-02 16:00:00', 16, 893);