# Logs Directory

This directory stores application logs for ExpiryTrack.

## Log Files

- `expirytrack.log` - Main application log
- `expirytrack_[date].log` - Daily rotated logs (if configured)

## Log Levels

- **DEBUG**: Detailed information for debugging
- **INFO**: General informational messages
- **WARNING**: Warning messages for potential issues
- **ERROR**: Error messages for failures

## Log Rotation

Logs may be automatically rotated based on:
- File size (default: 10MB)
- Daily rotation
- Maximum number of backup files

## Viewing Logs

To monitor logs in real-time:
```bash
# Windows
type logs\expirytrack.log

# Linux/Mac
tail -f logs/expirytrack.log
```

## Important Notes

- Logs contain debugging information but no sensitive data
- Clear old logs periodically to save disk space
- Check logs if you encounter any issues with data collection