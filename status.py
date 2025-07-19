import logging
logger = logging.getLogger(__name__)

async def send_status_update(client, message, identifier, content_info, status_type, extra_info=None, delete_previous=None):
    # Get original message ID and user info
    original_msg_id = int(identifier.split('_')[1])
    user_id = int(identifier.split('_')[0])
    user = await client.get_users(user_id)
    user_mention = user.mention
    
    # Build status message with content info
    content_title = content_info.get('title', 'Content')
    platform = content_info.get('platform', 'Unknown Platform')
    
    # Get chat link prefix - handle both Message object and chat_id
    chat_id = message.chat.id if hasattr(message, 'chat') else message
    chat = await client.get_chat(chat_id)
    chat_username = chat.username
    message_link_prefix = f"https://t.me/c/{str(chat_id)[4:]}" if str(chat_id).startswith('-100') else f"https://t.me/{chat_username}" if chat_username else f"tg://openmessage?chat_id={chat_id}"
    
    # Make content title clickable based on status
    if status_type in ["upload_complete_telegram", "upload_complete_drive"]:
        uploaded_msg_id = extra_info.get('uploaded_msg_id') if extra_info else None
        if uploaded_msg_id:
            clickable_title = f"[{content_title}]({message_link_prefix}/{uploaded_msg_id})"
        else:
            clickable_title = content_title
    else:
        clickable_title = f"[{content_title}]({message_link_prefix}/{original_msg_id})"
    
    # Construct status message with box-drawing characters
    message_lines = [
        "**┏━━━━━━━━━━━━━━━━━┓**",
        f"**⌬ User: {user_mention}**",
        f"> Platform: {platform}",
        f"**   ┏━━━━━━━━━━━━━━━━┛**",
        f"**   ┠ {clickable_title}**"
    ]
    
    # Add detailed status information based on type
    if status_type == "download_start":
        resolution = extra_info.get('resolution', '')
        if resolution:
            width, height = resolution.split('x')
            width = ''.join(c for c in width if c.isdigit())
            height = ''.join(c for c in height if c.isdigit())
            display_resolution = "1080p" if width == "1920" else f"{height}p"
        else:
            display_resolution = resolution
        audio_tracks = extra_info.get('audio_tracks', 0)
        message_lines.extend([
            f"**   ┠ Download Initiated**",
            f"**   ┠ Resolution: {display_resolution}**",
            f"**   ┗ Audio Tracks: {audio_tracks}**",
            "",
            "**⌬ Bot Stats**",
            f"**┖ Monitor Progress with /tasks**"
        ])
    
    elif status_type == "download_failed":
        message_lines.append(f"**   ┠ Download Failed**")
        if extra_info and 'limit_type' in extra_info:
            message_lines.extend([
                f"**   ┠ {extra_info['limit_type']} allocation restored**",
                f"**   ┗ Available:** {extra_info['limit']}"
            ])
        else:
            message_lines.append(f"**   ┗━━━━━━━━⌬**")
    
    elif status_type == "upload_start":
        file_size = extra_info.get('file_size', '0MB')
        # Store file size in content info for later use
        content_info['stored_file_size'] = file_size
        message_lines.extend([
            f"**   ┠ Upload Started**",
            f"**   ┗ Size: {file_size}**",
            "",
            "**⌬ Bot Stats**",
            f"**┖ Monitor Progress with /tasks**"
        ])
    
    elif status_type == "upload_complete_telegram":
        file_size = extra_info.get('file_size', content_info.get('stored_file_size', '0MB'))
        message_lines.append(f"**   ┠ Telegram Upload Complete**")
        if extra_info and 'limit_type' in extra_info:
            message_lines.extend([
                f"**   ┠ Size: {file_size}**",
                f"**   ┗ Remaining: {extra_info['limit_type']}: {extra_info['limit']}**",
                ">★─────────★"
            ])
        else:
            message_lines.extend([
                f"**   ┠ Size: {file_size}**", 
                f"**   ┗━━━━━━━━⌬**",
                ">★─────────★"
            ])
    
    elif status_type == "upload_complete_drive":
        file_size = extra_info.get('file_size', content_info.get('stored_file_size', '0MB'))
        message_lines.append(f"**   ┠ Drive Upload Complete**")
        if extra_info and 'limit_type' in extra_info:
            message_lines.extend([
                f"**   ┠ Size: {file_size}**",
                f"**   ┗ Tasks Left: {extra_info['limit']}**",
                ">★─────────★"
            ])
        else:
            message_lines.extend([
                f"**   ┠ Size: {file_size}**",
                f"**   ┗━━━━━━━━⌬**",
                ">★─────────★"
            ])
    elif status_type == "upload_unsuccessful":
        message_lines.append(f"**   ┠ Upload Failed**")
        if extra_info and 'error' in extra_info:
            message_lines.append(f"**   ┠ Error: {extra_info['error']}**")
        if extra_info and 'limit_type' in extra_info:
            message_lines.extend([
                f"**   ┠ {extra_info['limit_type']} allocation restored**",
                f"**   ┗ Available: {extra_info['limit']}**"
            ])
        else:
            message_lines.append(f"**   ┗━━━━━━━━⌬**")
    
    elif status_type == "stream_url_failed":
        message_lines.extend([
            f"**   ┠ Stream URL Failed**",
            f"**   ┗━━━━━━━━⌬**"
        ])
    
    # Handle previous message cleanup
    if delete_previous:
        try:
            await delete_previous.delete()
        except Exception as e:
            logger.error(f"Error removing previous status: {e}")
    
    # Send updated status message
    return await client.send_message(
        chat_id,
        "\n".join(message_lines),
        reply_to_message_id=original_msg_id,
        disable_web_page_preview=True
    )