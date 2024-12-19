from flask import Flask, request, render_template_string
import pandas as pd

app = Flask(__name__)

# Load the combined data
combined_data = pd.read_csv("combined_data.csv")

# HTML template for rendering the webpage
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Search by SIRET Number</title>
</head>
<body>
    <h1>Search by SIRET Number</h1>
    <form method="GET" action="/">
        <label for="siret">SIRET Number:</label>
        <input type="text" id="siret" name="siret" required>
        <button type="submit">Search</button>
    </form>
    {% if results %}
        <h2>Results:</h2>
        <table border="1">
            <tr>
                {% for col in results.columns %}
                    <th>{{ col }}</th>
                {% endfor %}
            </tr>
            {% for row in results.values %}
                <tr>
                    {% for cell in row %}
                        <td>{{ cell }}</td>
                    {% endfor %}
                </tr>
            {% endfor %}
        </table>
    {% endif %}
</body>
</html>
"""

@app.route("/", methods=["GET"])
def search():
    siret = request.args.get("siret")
    results = None
    if (siret):
        results = combined_data[(combined_data["Siret_Certificateur"] == siret) | (combined_data["Siret_Partenaire"] == siret)]
    return render_template_string(HTML_TEMPLATE, results=results)

if __name__ == "__main__":
    app.run(debug=True)
