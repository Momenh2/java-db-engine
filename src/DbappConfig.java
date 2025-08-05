import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

// Define a listener interface for configuration changes
interface ConfigChangeListener {
    void onConfigChange(String key, String value);
}

public class DbappConfig {
    private Properties properties;
    private List<ConfigChangeListener> listeners;

    // Constructor
    public DbappConfig(String configFile) {
        this.properties = new Properties();
        this.listeners = new ArrayList<>();
        // Load configuration settings from file
        loadSettingsFromFile(configFile);
    }

    // Load configuration settings from file
    private void loadSettingsFromFile(String configFile) {
        try (FileInputStream fis = new FileInputStream(configFile)) {
            properties.load(fis);
        } catch (IOException e) {
            System.err.println("Error loading configuration file: " + e.getMessage());
            // Handle error loading configuration file
        }
    }

    // Get configuration value by key
    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    // Set configuration value
    public void setProperty(String key, String value) {
        properties.setProperty(key, value);
        // Notify listeners about configuration change
        notifyListeners(key, value);
    }

    // Register a listener for configuration changes
    public void addConfigChangeListener(ConfigChangeListener listener) {
        listeners.add(listener);
    }

    // Notify all listeners about configuration change
    private void notifyListeners(String key, String value) {
        for (ConfigChangeListener listener : listeners) {
            listener.onConfigChange(key, value);
        }
    }
}
