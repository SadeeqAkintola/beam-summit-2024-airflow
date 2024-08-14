-- Delete the existing table if it exists
DROP TABLE IF EXISTS `beam-summit-2024-airflow.beam_2024_attendees.registrations`;

-- Create the new table
CREATE TABLE `beam-summit-2024-airflow.beam_2024_attendees.registrations` (
  name STRING,
  email STRING,
  location STRING,
  timestamp TIMESTAMP,
  file_location STRING,
  is_email_sent BOOLEAN
);
