import jdatetime

# Create a dictionary for Persian weekday names
weekdays_persian = {
    'Sunday': 'یکشنبه', 'Monday': 'دوشنبه', 'Tuesday': 'سه‌شنبه',
    'Wednesday': 'چهارشنبه', 'Thursday': 'پنج‌شنبه', 'Friday': 'جمعه',
    'Saturday': 'شنبه'
}

# Create a dictionary for Persian month names
months_persian = {
    'Farvardin': 'فروردین', 'Ordibehesht': 'اردیبهشت', 'Khordad': 'خرداد',
    'Tir': 'تیر', 'Mordad': 'مرداد', 'Shahrivar': 'شهریور',
    'Mehr': 'مهر', 'Aban': 'آبان', 'Azar': 'آذر', 'Dey': 'دی',
    'Bahman': 'بهمن', 'Esfand': 'اسفند'
}

# Function to get the formatted Persian date and time
def get_persian_date():
    # Get the current date and time in the Iranian calendar
    current_date = jdatetime.datetime.now()

    # Format the date and replace English weekdays and months with Persian
    formatted_date = current_date.strftime('%A %d %B %Y ساعت %H:%M')

    # Replace English weekday with Persian
    for english, persian in weekdays_persian.items():
        formatted_date = formatted_date.replace(english, persian)

    # Replace English month with Persian
    for english, persian in months_persian.items():
        formatted_date = formatted_date.replace(english, persian)

    return formatted_date
