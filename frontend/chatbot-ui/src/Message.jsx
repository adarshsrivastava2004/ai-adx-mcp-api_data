import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { Bot, User } from 'lucide-react'; // Icons

export default function Message({ role, text, typing }) {
  const isBot = role === 'bot';

  return (
    <div className={`message-row ${role}`}>
      
      {/* Avatar Icon */}
      <div className={`avatar ${isBot ? 'bot-avatar' : 'user-avatar'}`}>
        {isBot ? <Bot size={20} /> : <User size={20} />}
      </div>

      {/* Message Bubble */}
      <div className="message-bubble">
        {typing ? (
          <div className="typing-indicator">
             {/* 3 Dots SVG Animation */}
            <svg height="20" width="40">
              <circle className="typing-dot" cx="10" cy="10" r="3" />
              <circle className="typing-dot" cx="20" cy="10" r="3" />
              <circle className="typing-dot" cx="30" cy="10" r="3" />
            </svg>
          </div>
        ) : (
          /* Render Markdown (Tables, Bold, Lists) */
          <div className="markdown-content">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>
              {text}
            </ReactMarkdown>
          </div>
        )}
      </div>
    </div>
  );
}