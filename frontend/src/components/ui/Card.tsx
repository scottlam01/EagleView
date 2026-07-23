import styles from "./Card.module.css";
import type { ReactNode } from "react";

type CardProps = {
  children: ReactNode;
  className?: string;
};

export default function Card({ children, className = "" }: CardProps) {
  return (
    <section className={`${styles.card} ${className}`}>
      {children}
    </section>
  );
}