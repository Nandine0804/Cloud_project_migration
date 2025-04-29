import React, { useState } from 'react';
import axios from 'axios';
import './App.css';

function App() {
  const [jsonData, setJsonData] = useState('');
  const [file, setFile] = useState(null);
  const [error, setError] = useState('');
  const [migrationKey, setMigrationKey] = useState(''); // For Azure migration requests

  // Handle file upload
  const handleFileChange = (e) => {
    const file = e.target.files[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        setJsonData(e.target.result);
      };
      reader.readAsText(file);
      setFile(file);
    }
  };

  // Handle JSON data upload
  const handleSubmit = async () => {
    if (!jsonData && !file) {
      alert('Please upload a file or paste JSON data!');
      return;
    }

    const formData = new FormData();

    if (file) {
      formData.append('file', file);
    } else {
      const jsonBlob = new Blob([jsonData], { type: 'application/json' });
      formData.append('file', jsonBlob, 'data.json');
    }

    try {
      const res = await axios.post('http://localhost:5001/process-and-upload', formData);

      if (res.status === 200) {
        alert(res.data.message || 'Upload successful!');
        setJsonData('');
        setFile(null);
        setError('');
      }
    } catch (err) {
      const errorMessage = err.response?.data?.error || 
                          err.message || 
                          'Upload failed for unknown reason';
      alert(`Error: ${errorMessage}`);
      setError(errorMessage);
    }
  };

  // Handle migration request (AWS S3 to Azure Blob Storage)
  const handleMigrationRequest = async () => {
    if (!migrationKey) {
      alert('Please enter the file key (name) to migrate!');
      return;
    }

    try {
      const res = await axios.post('http://localhost:5001/fetch-from-s3', {
        file_key: migrationKey,
      });

      if (res.status === 200) {
        alert(res.data.message || 'Migration successful!');
        setMigrationKey('');
        setError('');
      }
    } catch (err) {
      const errorMessage = err.response?.data?.error || 
                          err.message || 
                          'Migration failed for unknown reason';
      alert(`Error: ${errorMessage}`);
      setError(errorMessage);
    }
  };

  return (
    <div className="container">
      <h1>Cloud Data Management</h1>

      {/* JSON Data Upload Section */}
      <div className="section">
        <h2>Upload JSON Data</h2>
        <textarea
          placeholder="Paste JSON data here"
          value={jsonData}
          onChange={(e) => setJsonData(e.target.value)}
        />
        <br />
        <input type="file" accept=".json" onChange={handleFileChange} />
        <br />
        <button onClick={handleSubmit}>Upload</button>
      </div>

      {/* Cloud-to-Cloud Migration Section */}
      <div className="section">
        <h2>Migrate File from AWS S3 to Azure Blob Storage</h2>
        <input
          type="text"
          placeholder="Enter file key (name) to migrate"
          value={migrationKey}
          onChange={(e) => setMigrationKey(e.target.value)}
        />
        <br />
        <button onClick={handleMigrationRequest}>Migrate</button>
      </div>

      {/* Error Display */}
      {error && <p className="error">{error}</p>}
    </div>
  );
}

export default App;