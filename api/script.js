// Function to populate the MSISDN dropdown
async function populateDropdown() {
    try {
        const response = await fetch('http://localhost:5050/msisdns');

        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        const data = await response.json();
        const dropdown = document.getElementById('msisdn_selec');

        // Clear previous options and add default option
        dropdown.innerHTML = '<option value="" disabled selected>Select MSISDN</option>';

        // Populate dropdown with available MSISDNs
        data.msisdns.forEach(msisdn => {
            const option = document.createElement('option');
            option.value = msisdn;
            option.text = msisdn;
            dropdown.add(option);
        });
    } catch (error) {
        console.error('Error fetching MSISDNs:', error);
    }
}

// Function to format and display usage data
function formatData(event_data) {
    const { category, usage_type, total, measure, start_time, cost_wak } = event_data;

    const box = document.createElement('div');
    box.classList.add('event_box');

    const header = document.createElement('h3');
    header.textContent = `${category.charAt(0).toUpperCase() + category.slice(1)} Event`;
    box.appendChild(header);

    const time = document.createElement('p');
    time.textContent = `Event Time - ${start_time}`;
    box.appendChild(time);

    const type = document.createElement('p');
    type.textContent = `Activity - ${usage_type.charAt(0).toUpperCase() + usage_type.slice(1)}`;
    box.appendChild(type);

    const usage = document.createElement('p');
    usage.textContent = `Billable Amount - ${total} ${measure.charAt(0).toUpperCase() + measure.slice(1)}`;
    box.appendChild(usage);

    const cost = document.createElement('p');
    cost.textContent = `Charge - WAK ${cost_wak}`;
    box.appendChild(cost);

    return box;
}

// Function to query usage data and load it into the page
async function queryAndLoadUsage() {
    const username = document.getElementById('username_in').value;
    const password = document.getElementById('password_in').value;
    const msisdn = document.getElementById('msisdn_selec').value;
    const query_url = `http://localhost:5050/data_usage?msisdn=${msisdn}&start_time=20240101000000&end_time=20240101235959`;
    
    if (!username || !password || !msisdn) {
        alert('Please fill in all fields!');
        return;
    }

    const headers = new Headers();
    headers.set('Authorization', 'Basic ' + btoa(username + ":" + password));

    const info = document.getElementById('info');
    info.innerHTML = '';  // Clear previous results

    try {
        const response = await fetch(query_url, {
            method: 'GET',
            headers: headers,
        });

        if (!response.ok) {
            throw new Error('Failed to fetch usage data');
        }

        const data = await response.json();
        data.usage.forEach(event_data => {
            info.append(formatData(event_data));
        });
    } catch (error) {
        console.error('Error fetching usage data:', error);
        info.innerHTML = '<p class="error-message">Failed to load data. Please try again later.</p>';
    }
}

// Event listener for the query button
document.getElementById('query_btn').addEventListener('click', queryAndLoadUsage);

// Load MSISDNs when the page loads
window.onload = populateDropdown;
