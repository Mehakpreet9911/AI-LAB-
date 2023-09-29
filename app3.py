from flask import Flask, request, render_template
import os
import pandas as pd
from process_logs1 import process_csv_and_logs  # Import the function

app3 = Flask(__name__)

UPLOAD_FOLDER = 'uploads'  # Corrected folder name
app3.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# ...


@app3.route('/')
def index():
    return render_template('upload.html')

@app3.route('/uploads', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return "No file part"

    file = request.files['file']
    


    if file.filename == '':
        return "No selected file"

    if file:
        filename = os.path.join(app3.config['UPLOAD_FOLDER'], file.filename)
        print("File is being saved at:", filename)  # Add this line for debugging
        file.save(filename)

        # Call the process_csv_and_logs function and pass the DataFrame to it
        processed_data = process_csv_and_logs(filename)

        # Render the result.html template and pass the processed data
        return render_template('result.html', data=processed_data)

if __name__ == '__main__':
    app3.run(debug=True)


