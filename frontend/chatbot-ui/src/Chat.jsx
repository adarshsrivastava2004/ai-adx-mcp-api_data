import { useEffect, useRef, useState } from "react";
import Message from "./Message";
import { Send, Sparkles } from "lucide-react";

const API_URL = "http://127.0.0.1:8000/chat";

export default function Chat() {
  const [messages, setMessages] = useState([
    { role: "bot", text: "Hello! I'm your Enterprise Data Assistant.How can I help you? 🌪️" }
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);

  // Auto-scroll to bottom
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  async function sendMessage() {
    if (!input.trim() || loading) return;

    const userText = input;
    // Add User Message
    setMessages(prev => [...prev, { role: "user", text: userText }]);
    setInput("");
    setLoading(true);

    try {
      const res = await fetch(API_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: userText })
      });

      if (!res.ok) throw new Error("Network Error");

      const data = await res.json();
      
      // Add Bot Message
      setMessages(prev => [...prev, { role: "bot", text: data.reply }]);
    } catch (e) {
      setMessages(prev => [
        ...prev, 
        { role: "bot", text: "⚠️ **System Error:** Unable to reach the backend. Is the server running?" }
      ]);
    } finally {
      setLoading(false);
    }
  }

  // Handle "Enter" key
  const handleKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  return (
    <div className="chat-root">
      {/* Header */}
      <div className="chat-header">
        <Sparkles className="text-blue-400" size={24} />
        <span>Enterprise ADX Agent</span>
      </div>

      {/* Messages Area */}
      <div className="chat-body">
        {messages.map((m, i) => (
          <Message key={i} role={m.role} text={m.text} />
        ))}
        {loading && <Message role="bot" typing={true} />}
        <div ref={bottomRef} />
      </div>

      {/* Input Area */}
      <div className="chat-input-area">
        <div className="input-container">
          <textarea
            rows="1"
            value={input}
            placeholder="Query your data..."
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
          />
          <button 
            className="send-btn" 
            onClick={sendMessage} 
            disabled={loading || !input.trim()}
          >
            <Send size={18} />
          </button>
        </div>
        <div style={{ textAlign: "center", marginTop: "8px", fontSize: "12px", color: "#64748b" }}>
          AI can make mistakes. Please verify critical data.
        </div>
      </div>
    </div>
  );
}