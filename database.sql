-- This is part of Enphase-Inverter-Analyzer <https://github.com/rbroders/Enphase-Inverter-Analyzer>
-- Copyright (C) 2025 RBroders!
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU General Public License version 3 as
-- published by the Free Software Foundation.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program.  If not, see <https://www.gnu.org/licenses/>.

--
-- The program inverter_capture.py will create this table automatically
-- but it is useful to have the schema here for reference.
--

-- MySQL/MariaDB schema for Enphase API v1 production data
CREATE TABLE IF NOT EXISTS `APIV1ProductionInverters` (
 `LastReportDate`   TIMESTAMP          NOT NULL COMMENT 'lastReportDate (timestamp of the report)',
 `SerialNumber`     BIGINT   UNSIGNED  NOT NULL COMMENT 'serialNumber (12 digits)',
 `Watts`            SMALLINT UNSIGNED  NOT NULL COMMENT 'lastReportWatts',
PRIMARY KEY (`LastReportDate`, `SerialNumber`)
COMMENT 'This table holds responses from the Enphase gateway /api/v1/production/inverters endpoint (except MaxWatts)'
);

-- SQLite3 schema does not support the `COMMENT` syntax, so we use -- a separate comment for each column
CREATE TABLE IF NOT EXISTS `APIV1ProductionInverters` (
 `LastReportDate`   TIMESTAMP          NOT NULL, --lastReportDate (timestamp of the report)
 `SerialNumber`     BIGINT   UNSIGNED  NOT NULL, --serialNumber (12 digits)
 `Watts`            SMALLINT UNSIGNED  NOT NULL, --lastReportWatts
PRIMARY KEY (`LastReportDate`, `SerialNumber`) --This table holds responses from the Enphase gateway /api/v1/production/inverters endpoint (except MaxWatts)
) WITHOUT ROWID;
