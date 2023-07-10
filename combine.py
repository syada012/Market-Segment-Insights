import pandas as pd
import sys
import os

# Get the input directory from the command-line arguments
input_dir = sys.argv[1]

# Load the csv files into dataframes
df_reservations = pd.read_csv(os.path.join(input_dir, 'customer-reservations.csv'))
df_booking = pd.read_csv(os.path.join(input_dir, 'hotel-booking.csv'))

# Create a dictionary to map month numbers to month names
month_mapping = { 
                 1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June',
                 7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'
}

# Replace the 'arrival_month' column values using the dictionary
df_reservations['arrival_month'] = df_reservations['arrival_month'].map(month_mapping)

# Ensure consistent column naming across both dataframes
df_booking = df_booking.rename(columns={
    'Booking_status': 'booking_status',
    'arrival_date_day_of_month': 'arrival_date',
    'avg_price_per_room': 'avg_price_per_room',
    'stays_in_week_nights': 'stays_in_week_nights',
    'stays_in_weekend_nights': 'stays_in_weekend_nights',
    'lead_time': 'lead_time',
    'arrival_year': 'arrival_year',
    'arrival_month': 'arrival_month',
    'market_segment_type': 'market_segment_type'
})

# Perform the concatenation operation
df_combined = pd.concat([df_reservations, df_booking], axis=0)

# Fill the missing values with 0
df_combined = df_combined.fillna(0)

# Write the combined dataframe to a CSV file
df_combined.to_csv(os.path.join(input_dir, 'combinedDataset.csv'), index=False)

